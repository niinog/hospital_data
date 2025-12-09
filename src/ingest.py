import json
from pathlib import Path

import pandas as pd

from .config import RAW_CSV_DIR, RAW_FHIR_DIR


# ---------- CSV LOADERS ----------

def load_patients_csv() -> pd.DataFrame:
    """
    Load patients.csv from the raw CSV folder.

    """
    path = RAW_CSV_DIR / "patients.csv"
    df = pd.read_csv(path)
    return df


def load_encounters_csv() -> pd.DataFrame:
    """
    Load encounters.csv from the raw CSV folder.

    """
    path = RAW_CSV_DIR / "encounters.csv"
    df = pd.read_csv(path)
    return df


def load_providers_csv() -> pd.DataFrame:
    """
    Load providers.csv from the raw CSV folder.

    """
    path = RAW_CSV_DIR / "providers.csv"
    df = pd.read_csv(path)
    return df


def load_organizations_csv() -> pd.DataFrame:
    """
    Load organizations.csv from the raw CSV folder.

    """
    path = RAW_CSV_DIR / "organizations.csv"
    df = pd.read_csv(path)
    return df


def load_medications_csv() -> pd.DataFrame:
    """
    Load medications.csv from the raw CSV folder.

    """
    path = RAW_CSV_DIR / "medications.csv"
    df = pd.read_csv(path)
    return df


# ---------- JSON (FHIR) LOADER ----------

def load_observations_fhir() -> pd.DataFrame:
    """
    Load Observation resources from all FHIR JSON files in RAW_FHIR_DIR.

      - opens each JSON file,
      - iterates over Bundle.entry,
      - keeps only resources where resourceType == 'Observation',
      - flattens them into a single pandas DataFrame.

    Returns: DataFrame with one row per Observation.
    """
    rows = []

    # Iterate all JSON files under RAW_FHIR_DIR
    for file in Path(RAW_FHIR_DIR).glob("*.json"):
        with open(file, "r") as f:
            bundle = json.load(f)

        # Each file is usually a FHIR Bundle with an "entry" list
        for entry in bundle.get("entry", []):
            resource = entry.get("resource", {})
            if resource.get("resourceType") == "Observation":
                rows.append(resource)

    if not rows:
        # no observations found; return empty DataFrame
        return pd.DataFrame()

    # Flatten nested JSON into columns
    df = pd.json_normalize(rows)
    return df
