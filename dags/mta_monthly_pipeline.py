import os
import logging
from datetime import datetime, timedelta, date
from airflow.decorators import dag, task

DATASETS = {
    "ridership": {
        "id": "5wq4-mkjj",
        "timestamp_col": "transit_timestamp",      # hourly; ISO datetime string
        "date_format": "T00:00:00",                # appended to build SoQL timestamp
        "critical_cols": [
            "transit_timestamp",
            "station_complex",
            "station_complex_id",
            "ridership",
        ],
        "min_rows": 200_000,                        # ~500 complexes × 24h × 30d
        "has_station_check": True,
    },
    "apt": {
        "id": "s4u6-t435",
        "timestamp_col": "month",                   # monthly date; no time component
        "date_format": "",                          # plain ISO date, no time suffix
        "critical_cols": [
            "month",
            "line",
            "division",
            "additional_platform_time",             # sodapy snake_cases the header
            "num_passengers",
        ],
        "min_rows": 20,                             # ~25 lines × 2 periods = ~50 rows
        "has_station_check": False,                 # APT is by line, not station_complex
    },
}


default_args = {
    "owner": "mta-peek",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="mta_monthly_pipeline",
    schedule="0 6 1 * *",
    start_date=datetime(2026,1,1),
    catchup=False,
    default_args=default_args,
    tags=["mta", "ridership", "monthly"],
)
def mta_pipeline():

    @task()
    def ingest_from_api(dataset_id: str, dataset_name: str) -> str:
        """Pull latest month's data from MTA dataset and write to parquet"""
        import pandas as pd
        from sodapy import Socrata

        log = logging.getLogger(__name__)

        token = os.environ.get("SOCRATA_APP_TOKEN")
        raw_dir = os.environ["DATA_RAW"]
        config = DATASETS['dataset_name']

        today = date.today()

        # get previous month's timeframe
        # if it's the month of January
        if today.month == 1:
            # refer to previous month as December of previous year
            first_of_prior = today.replace(year=today.year-1, month=12, day=1)
        else:
            first_of_prior = today.replace(month=today.month-1, day=1)
        first_of_this = today.replace(day=1)

        # format timestamp based on the dataset's corresponding format
        # per above config
        ts_col = config["timestamp_col"]
        date_fmt = config["date_format"]

        # specify filter using above timestamps
        where_clause = (
            f"{ts_col} >= '{first_of_prior.isoformat()}{date_fmt}'"
            f"AND {ts_col} < '{first_of_this.isoformat()}{date_fmt}'"
        )

        # fetch data from MTA Socrata API
        log.info("[%s] Fetching %s to %s | where: %s", dataset_name, first_of_prior, first_of_this, where_clause)

        client = Socrata("data.ny.gov", token, timeout=120)
        results = client.get_all(dataset_id, where=where_clause)
        
        log.info('[%s] Fetching complete, now processing into DataFrame', dataset_name)
        df = pd.DataFrame.from_records(results)
        client.close()
        log.info("[%s] API returned %d rows", dataset_name, len(df))

        out_path = f"{raw_dir}/{dataset_name}_{first_of_prior.strftime('%Y_%m')}.parquet"
        df.to_parquet(out_path, index=False)
        log.info("[%s] Parquet written", dataset_name)
        return out_path
    
    @task()
    def run_data_quality(raw_path: str, dataset_name: str) -> dict:
        """Validate row counts, nulls, and station names (ridership data only)"""
        import pandas as pd
        processed_dir = os.environ["DATA_PROCESSED"]

        config = DATASETS[dataset_name]
        df = pd.read_parquet(raw_path)

        # check row counts sufficient
        if len(df) < config["min_rows"]:
            raise ValueError(f"[{dataset_name}] row count too low: {len(df)}")
        
        # check null values for critical columns 
        # that subsequent computations depend on
        for col in config["critical_cols"]:
            null_count = df[col].isna().sum()
            if null_count > 0:
                raise ValueError(f"[{dataset_name}] null values in '{col}': {null_count}")
        
        # station name check for ridership data only
        if config["has_station_check"]:
            known_stations = set(
                pd.read_parquet(f"{processed_dir}/station_name_check/")["station_complex"]
            )
            unexpected = set(df["station_complex"]) - known_stations
            if len(unexpected) > 10:
                raise ValueError(
                    f"[{dataset_name}] has too many unexpected stations: {unexpected}"
                )
        return {"dataset": dataset_name, "rows": len(df), "path": raw_path}
    
    @task()
    def append_apt_to_lake(quality_result: dict) -> str:
        """Append latest APT data to exisiting processed APT data lake"""
        import pandas as pd

        processed_dir = os.environ["DATA_PROCESSED"]
        raw_path = quality_result["path"]

        df = pd.read_parquet(raw_path)
        df["month"] = pd.to_datetime(df["month"])
        df["year"] = df["month"].dt.year
        df["month_num"] = df["month"].dt.month

        # output distinct parquet file inside the existing APT data directory
        today = date.today()
        if today.month == 1:
            month_label = f"{today.year - 1}_12"
        else:
            month_label = f"{today.year}_{today.month - 1:02d}"

        out_path = f"{processed_dir}/apt/apt_{month_label}.parquet"
        df.to_parquet(out_path, index=False)
        return out_path
    
    @task()
    def transform_ridership(ridership_quality: dict, apt_lake_path: str) -> str:
        """Automatically run PySpark transformation script for all fetched datasets"""
        import subprocess

        raw_path = ridership_quality["path"]
        result = subprocess.run(
            ["python", "spark_jobs/transform.py", "--input", raw_path],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Spark transform failed:\n{result.stderr}")
        return raw_path
    
    def write_audit_log(quality_results: list[dict]):
        """Append data quality audit log with one record per fetched dataset"""
        import pandas as pd

        log_path = "data_quality/audit_log.csv"
        records = pd.DataFrame([
            {
                "run_date": datetime.utcnow.isoformat(),
                "dataset": r["dataset"],
                "rows_ingested": r["rows"],
                "source_file": r["path"],
                "status": "success",
            }
            for r in quality_results
        ])
        records.to_csv(
            log_path, mode='a', header=not os.path.exists(log_path),
            index=False
        )

    raw_ridership = ingest_from_api.override(task_id="ingest_ridership")(
        dataset_id=DATASETS["ridership"]["id"],
        dataset_name="ridership"
    )
    raw_apt = ingest_from_api.override(task_id="ingest_apt")(
        dataset_id=DATASETS["apt"]["id"],
        dataset_name="apt"
    )

    quality_ridership = run_data_quality.override(task_id="quality_ridership")(
        raw_path=raw_ridership,
        dataset_name="ridership"
    )
    quality_apt = run_data_quality.override(task_id="quality_apt")(
        raw_path=raw_apt,
        dataset_name="apt"
    )

    apt_lake = append_apt_to_lake(quality_result=quality_apt)

    transformed = transform_ridership(
        ridership_quality=quality_ridership,
        apt_lake_path=apt_lake,
    )

    write_audit_log([quality_ridership, quality_apt])

mta_pipeline()