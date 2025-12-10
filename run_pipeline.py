from src.config import PROCESSED_DIR
from src.utils import ensure_dir

from src.ingest import (
    load_patients_csv,
    load_encounters_csv,
    load_providers_csv,
    load_organizations_csv,
    load_medications_csv,
    load_observations_fhir,
)

from src.transform import (
    clean_patients,
    clean_encounters,
    clean_providers,
    clean_organizations,
    clean_medications,
    clean_observations_fhir,
    join_patients_observations,
    pivot_observations_to_features,
)


# Which observation types we want as features:
TOP_CODES = [
    "Body Height",
    "Body Weight",
    "Body Mass Index",
    "Blood Pressure",
    "Heart rate",
    "Glucose",
]


def main():
    
    ensure_dir(PROCESSED_DIR)

    # ---------- 1) Load raw CSVs ----------
    patients_raw = load_patients_csv()
    encounters_raw = load_encounters_csv()
    providers_raw = load_providers_csv()
    orgs_raw = load_organizations_csv()
    meds_raw = load_medications_csv()

    # ---------- 2) Load raw FHIR JSON ----------
    obs_raw = load_observations_fhir()

    # ---------- 3) Clean CSV tables ----------
    dim_patient = clean_patients(patients_raw)
    fact_encounter = clean_encounters(encounters_raw)
    dim_provider = clean_providers(providers_raw)
    dim_organization = clean_organizations(orgs_raw)
    fact_medication = clean_medications(meds_raw)

    # ---------- 4) Clean Observations + join to patients ----------
    obs_clean = clean_observations_fhir(obs_raw)
    fact_patient_obs = join_patients_observations(dim_patient, obs_clean)

    # ---------- 5) Pivot observations into per-patient features ----------
    patient_features = pivot_observations_to_features(
        fact_patient_obs,
        top_codes=TOP_CODES,
    )

    # ---------- 6) Write all cleaned tables to data/processed ----------
    dim_patient.to_csv(PROCESSED_DIR / "dim_patient.csv", index=False)
    fact_encounter.to_csv(PROCESSED_DIR / "fact_encounter.csv", index=False)
    dim_provider.to_csv(PROCESSED_DIR / "dim_provider.csv", index=False)
    dim_organization.to_csv(PROCESSED_DIR / "dim_organization.csv", index=False)
    fact_medication.to_csv(PROCESSED_DIR / "fact_medication.csv", index=False)

    obs_clean.to_csv(PROCESSED_DIR / "fact_observation.csv", index=False)
    fact_patient_obs.to_csv(PROCESSED_DIR / "fact_patient_observation.csv", index=False)
    patient_features.to_csv(PROCESSED_DIR / "patient_observation_features.csv", index=False)

    print("All cleaned tables written to:", PROCESSED_DIR)


if __name__ == "__main__":
    main()
