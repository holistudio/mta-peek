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
            "borough",
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
    def transform_and_append(raw_path: str, dataset_name: str) -> str:
        """Automatically run PySpark transformation script for given dataset"""
        import subprocess
        result = subprocess.run(
            [
                "python", "spark_jobs/transform.py",
                "--input", raw_path,
            ]
        )
        if result.returncode != 0:
            raise RuntimeError(f"Spark job failed for {dataset_name}:\n{result.stderr}")

        return raw_path

    raw_ridership = ingest_from_api.override(task_id="ingest_ridership")(
        dataset_id=DATASETS["ridership"]["id"],
        dataset_name="ridership"
    )
    raw_apt = ingest_from_api.override(task_id="ingest_apt")(
        dataset_id=DATASETS["apt"]["id"],
        dataset_name="apt"
    )

    transformed_ridership = transform_and_append.override(task_id="transform_ridership")(
        raw_path=raw_ridership,
        dataset_name="ridership"
    )
    transformed_apt = transform_and_append.override(task_id="transform_apt")(
        raw_path=raw_apt,
        dataset_name="apt"
    )


mta_pipeline()