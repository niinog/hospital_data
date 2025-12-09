import pandas as pd


def clean_observations_fhir(obs_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw FHIR Observation resources so they are joinable to CSV tables.

    Input: DataFrame from ingest.load_observations_fhir()
      Important columns in obs_raw:
        - 'id'
        - 'subject.reference'        (patient reference, contains 'urn:uuid:<patient_id>')
        - 'encounter.reference'      (encounter reference, contains 'urn:uuid:<encounter_id>')
        - 'effectiveDateTime'
        - 'code.coding'              (list of dicts with code info)
        - 'code.text'
        - 'valueQuantity.value'
        - 'valueQuantity.unit'

    Output: obs_clean with columns:
        - observation_id
        - patient_id
        - encounter_id
        - observation_datetime
        - code_system
        - code
        - code_display
        - result_value
        - result_unit
    """
    if obs_raw.empty:
        return obs_raw

    # 1) Keep only the columns we care about
    cols = [
        "id",
        "subject.reference",
        "encounter.reference",
        "effectiveDateTime",
        "code.coding",
        "code.text",
        "valueQuantity.value",
        "valueQuantity.unit",
    ]
    obs = obs_raw[cols].copy()

    # 2) Extract clean patient_id and encounter_id (strip "urn:uuid:")
    obs["patient_id"] = obs["subject.reference"].str.replace(
        "urn:uuid:", "", regex=False
    )
    obs["encounter_id"] = obs["encounter.reference"].str.replace(
        "urn:uuid:", "", regex=False
    )

    # 3) Parse observation datetime
    obs["observation_datetime"] = pd.to_datetime(
        obs["effectiveDateTime"], errors="coerce"
    )

    # 4) Extract code_system, code, display from code.coding
    def extract_coding_fields(codings):
        """
        Take the value of code.coding (usually a list of dicts) and return
        (system, code, display). If missing, returns (None, None, None).
        """
        if isinstance(codings, list) and len(codings) > 0 and isinstance(codings[0], dict):
            c = codings[0]
            return c.get("system"), c.get("code"), c.get("display")
        return (None, None, None)

    coding_df = obs["code.coding"].apply(extract_coding_fields).apply(pd.Series)
    coding_df.columns = ["code_system", "code", "code_display"]

    # Attach coding fields
    obs = pd.concat([obs, coding_df], axis=1)

    # 5) Build final tidy Observation table
    obs_clean = obs[
        [
            "id",
            "patient_id",
            "encounter_id",
            "observation_datetime",
            "code_system",
            "code",
            "code_display",
            "valueQuantity.value",
            "valueQuantity.unit",
        ]
    ].rename(
        columns={
            "id": "observation_id",
            "valueQuantity.value": "result_value",
            "valueQuantity.unit": "result_unit",
        }
    )

    return obs_clean

import pandas as pd

# ---------- CSV CLEANERS ----------

def clean_patients(patients_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean patients.csv and prepare dim_patient.

    Output columns:
      - patient_id (from Id)
      - birthdate
      - age_years  (derived from birthdate)
      - gender
      - state
      - healthcare_expenses
      - healthcare_coverage
    """
    df = patients_raw.copy()
    df.columns = df.columns.str.lower()

    # Parse dates
    df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")

    # Age in years at a reference date
    reference_date = pd.Timestamp("2020-01-01")
    df["age_years"] = (
        (reference_date - df["birthdate"]).dt.days / 365.25
    ).round(1)

    # Standardized ID column
    df["patient_id"] = df["id"]

    cols_to_keep = [
        "patient_id",
        "birthdate",
        "age_years",
        "gender",
        "state",
        "healthcare_expenses",
        "healthcare_coverage",
    ]
    
    cols_to_keep = [c for c in cols_to_keep if c in df.columns]

    return df[cols_to_keep]

def clean_encounters(encounters_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean encounters.csv and prepare fact_encounter.

    Keeps:
      - encounter_id
      - patient_id
      - organization_id
      - provider_id
      - start, stop
      - length_of_stay_hours
      - encounterclass, code, description
      - base_encounter_cost, total_claim_cost, payer_coverage
    """
    df = encounters_raw.copy()
    df.columns = df.columns.str.lower()

    # Parse datetimes
    for col in ["start", "stop"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df["encounter_id"] = df["id"]
    df["patient_id"] = df["patient"]
    df["organization_id"] = df["organization"]
    df["provider_id"] = df["provider"]

    # Length of stay in hours
    df["length_of_stay_hours"] = (
        df["stop"] - df["start"]
    ).dt.total_seconds() / 3600

    cols_to_keep = [
        "encounter_id",
        "patient_id",
        "organization_id",
        "provider_id",
        "start",
        "stop",
        "length_of_stay_hours",
        "encounterclass",
        "code",
        "description",
        "base_encounter_cost",
        "total_claim_cost",
        "payer_coverage",
    ]
    cols_to_keep = [c for c in cols_to_keep if c in df.columns]

    return df[cols_to_keep]
def clean_providers(providers_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean providers.csv and prepare dim_provider.

    Keeps:
      - provider_id
      - organization_id
      - name
      - gender
      - speciality
      - state
      - zip
      - utilization
    """
    df = providers_raw.copy()
    df.columns = df.columns.str.lower()

    df["provider_id"] = df["id"]
    df["organization_id"] = df["organization"]

    cols_to_keep = [
        "provider_id",
        "organization_id",
        "name",
        "gender",
        "speciality",
        "state",
        "zip",
        "utilization",
    ]
    cols_to_keep = [c for c in cols_to_keep if c in df.columns]

    return df[cols_to_keep]
def clean_organizations(org_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean organizations.csv and prepare dim_organization.

    Keeps:
      - organization_id
      - name
      - city
      - state
      - zip
      - revenue
      - utilization
    """
    df = org_raw.copy()
    df.columns = df.columns.str.lower()

    df["organization_id"] = df["id"]

    cols_to_keep = [
        "organization_id",
        "name",
        "city",
        "state",
        "zip",
        "revenue",
        "utilization",
    ]
    cols_to_keep = [c for c in cols_to_keep if c in df.columns]

    return df[cols_to_keep]
def clean_medications(meds_raw: pd.DataFrame) -> pd.DataFrame:
    """

    Keeps:
      - medication_id (synthetic)
      - patient_id
      - encounter_id
      - start, stop
      - code, description
      - base_cost, totalcost, payer_coverage
    """
    df = meds_raw.copy()
    df.columns = df.columns.str.lower()

    # Parse datetimes
    for col in ["start", "stop"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Synthetic primary key
    df = df.reset_index().rename(columns={"index": "medication_id"})

    df["patient_id"] = df["patient"]
    df["encounter_id"] = df["encounter"]

    cols_to_keep = [
        "medication_id",
        "patient_id",
        "encounter_id",
        "start",
        "stop",
        "code",
        "description",
        "base_cost",
        "totalcost",
        "payer_coverage",
    ]
    cols_to_keep = [c for c in cols_to_keep if c in df.columns]

    return df[cols_to_keep]

def join_patients_observations(patients: pd.DataFrame, observations: pd.DataFrame) -> pd.DataFrame:
    """
    Join cleaned FHIR observations (obs_clean) with cleaned patients.

    Input:
      patients     = output of clean_patients()
      observations = output of clean_observations_fhir()

    Output: fact table with both patient demographics and observation details.
    """
    df_pat = patients.copy()
    df_obs = observations.copy()

    # obs_clean already has patient_id, so we can merge directly
    merged = df_obs.merge(df_pat, on="patient_id", how="left")

    # Select useful columns from BOTH sides
    cols_to_keep = [
        "observation_id",
        "patient_id",
        "encounter_id",
        "observation_datetime",
        "code_system",
        "code",
        "code_display",        # what was measured (e.g., Body Height)
        "result_value",        # numeric result
        "result_unit",         # e.g. cm, kg
        "gender",
        "state",
        "age_years",
        "healthcare_coverage",
    ]
    cols_to_keep = [c for c in cols_to_keep if c in merged.columns]

    return merged[cols_to_keep]

def pivot_observations_to_features(
    patient_obs: pd.DataFrame,
    top_codes: list[str],
) -> pd.DataFrame:
    """
    Pivot selected observation types (code_display) into numeric features
    on a per-patient basis.

    """
    df = patient_obs.copy()

    df["code_display"] = df["code_display"].astype(str).str.strip()

    normalized_codes = [c.strip() for c in top_codes]

    df = df[df["code_display"].isin(normalized_codes)]

    # Ensure result_value is numeric
    df["result_value"] = pd.to_numeric(df["result_value"], errors="coerce")

    pivoted = (
        df.pivot_table(
            index="patient_id",
            columns="code_display",
            values="result_value",
            aggfunc="last",   # last measurement per patient
        )
        .reset_index()
    )

    pivoted.columns.name = None

    return pivoted
