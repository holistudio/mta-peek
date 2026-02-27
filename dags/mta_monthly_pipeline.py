import os
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

