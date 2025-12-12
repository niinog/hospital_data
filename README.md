# Hospital Data – End-to-End Data Pipeline

A small, repeatable data pipeline that turns raw Synthea hospital data into
clean analytical tables and loads them into BigQuery.

The project demonstrates:

- Ingestion of CSV + FHIR JSON files  
- Cleaning, transformation, and validation in Python (pandas)  
- Creation of fact and dimension tables  
- Optional loading to BigQuery for analysis  
- Basic data governance (versioning, logging, and documentation)

---

## 1. Project Overview

The goal is to build a **simple, auditable pipeline** that can be re-run whenever
new Synthea data is generated.

High-level flow:

1. **Ingest** raw CSV and FHIR JSON files from `data/raw`.
2. **Transform & clean** them into tidy tables.
3. **Validate** key tables (patients, encounters) and log any issues.
4. **Write** cleaned tables to `data/processed`.
5. **Load** processed tables into BigQuery dataset `patient_analysis`
   in the GCP project `final-project-ores5160`.

---

## 2. Data Flow / Pipeline Design

```text
Raw files (Synthea output)
    ├── data/raw/csv/csv/*.csv
    └── data/raw/json/*.json

Ingestion (src/ingest.py)
    ├── load_patients_csv()
    ├── load_encounters_csv()
    ├── load_providers_csv()
    ├── load_organizations_csv()
    ├── load_medications_csv()
    └── load_observations_fhir()

Transformation (src/transform.py)
    ├── clean_patients()
    ├── clean_encounters()
    ├── clean_providers()
    ├── clean_organizations()
    ├── clean_medications()
    ├── clean_observations_fhir()
    ├── join_patients_observations()
    └── pivot_observations_to_features()

Validation (src/validate.py)
    ├── validate_dim_patient()
    └── validate_fact_encounter()

Output (run_pipeline.py)
    └── data/processed/*.csv

Load to BigQuery
    └── Dataset: final-project-ores5160.patient_analysis

## 3. Repository Structure

```text
hospital_data/
├── data/
│   ├── raw/
│   │   ├── csv/csv/
│   │   │   ├── patients.csv
│   │   │   ├── encounters.csv
│   │   │   ├── medications.csv
│   │   │   ├── providers.csv
│   │   │   └── organizations.csv
│   │   └── json/
│   │       └── observations/*.json      # FHIR Observation bundles
│   └── processed/                       # pipeline outputs (created by script)
├── creds/
│   └── final-project-ores5160-70832cc9a26f.json   # service account (git-ignored)
├── src/
│   ├── config.py          # paths (RAW_DIR, PROCESSED_DIR, etc.)
│   ├── ingest.py          # all load_* functions
│   ├── transform.py       # all clean_* and join/pivot functions
│   ├── validate.py        # validation & logging
│   ├── utils.py           # ensure_dir, small helpers
│   └── __init__.py
├── run_pipeline.py        # runs full ingest → transform → validate → write
├── requirements.txt
├── README.md
└── .gitignore

