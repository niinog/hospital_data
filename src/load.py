# src/load.py

from pathlib import Path
import pandas as pd
from google.cloud import bigquery

# ---------- Paths ----------

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed" 

# Your service-account JSON (already in creds/ in your repo)
CREDS_PATH = BASE_DIR / "creds" / "final-project-ores5160-70832cc9a26f.json"

# GCP project + dataset
PROJECT_ID = "final-project-ores5160"   
DATASET_ID = "hospital_data"            


# ---------- BigQuery client ----------

client = bigquery.Client.from_service_account_json(
    CREDS_PATH,
    project=PROJECT_ID,
)


# ---------- Loader ----------

def load_to_bigquery(filename: str, table_name: str) -> None:
    """
    Read a CSV from data/processed and upload it to BigQuery as:
      <PROJECT_ID>.<DATASET_ID>.<table_name>

    filename  - the CSV file name inside data/processed
    table_name - the BigQuery table name
    """
    csv_path = PROCESSED_DIR / filename

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # overwrite table
        autodetect=True,                     # infer schema from df
    )

    print(f"Uploading {len(df):,} rows from {csv_path} to {table_id} …")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # wait until job finishes
    print(f"Done: loaded {len(df):,} rows into {table_id}")
