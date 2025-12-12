# src/upload_all_to_bq.py

from src.load import load_to_bigquery


def main():
    # Adjust filenames and table names to your actual processed CSVs
    load_to_bigquery("patients_clean.csv",      "dim_patient")
    load_to_bigquery("encounters_clean.csv",    "fact_encounter")
    load_to_bigquery("providers_clean.csv",     "dim_provider")
    load_to_bigquery("organizations_clean.csv", "dim_organization")
    load_to_bigquery("patient_obs_clean.csv",   "fact_observation")
    # add/remove lines as needed


if __name__ == "__main__":
    main()
