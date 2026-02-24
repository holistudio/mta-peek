# Installation and Setup

## Installation

`.env` file (example)

```bash
AIRFLOW_HOME=/home/<youruser>/mta-peek/airflow
SOCRATA_APP_TOKEN=your_token_here
DATA_RAW=/home/<youruser>/mta-peek/data/raw
DATA_PROCESSED=/home/<youruser>/mta-peek/data/processed
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

`conda` environment

```bash
conda create -n mta-peek python=3.11 -y
conda activate mta-peek
(uv) pip install -r requirements.txt
```

Airflow

```bash
AIRFLOW_VERSION=3.1.7
PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install pyspark==4.1.1 pyarrow==23.0.1 pandas==3.0.1 numpy==2.4.2 sodapy==2.2.0 \
    matplotlib==3.10.8 seaborn==0.13.2 plotly==6.5.2 \
    ipykernel==7.2.0 ipywidgets==8.1.8 nbformat==5.10.4
```

## Setup

After installation, run these terminal commands before executing other code.

Activate conda environment

```bash
conda activate mta-peek
```
Load environment variables

```bash
set -a && source .env && set +a
```