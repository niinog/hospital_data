# src/validate.py

from pathlib import Path
import logging
import pandas as pd
import numpy as np


# ------------------ Logging setup ------------------

BASE_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_PATH = LOG_DIR / "validation.log"

logger = logging.getLogger("validation")
logger.setLevel(logging.INFO)

# Avoid adding handlers twice if module is reloaded
if not logger.handlers:
    file_handler = logging.FileHandler(LOG_PATH)
    console_handler = logging.StreamHandler()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(fmt)
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# ------------------ Helper function ------------------

def _log_error(errors: list[str], message: str) -> None:
    errors.append(message)
    logger.error(message)


# ------------------ Patient dimension ------------------

def validate_dim_patient(df: pd.DataFrame) -> bool:
    """
    Basic validation for the cleaned patient table (dim_patient).

    Checks:
      - required columns exist
      - birthdate is datetime (or coercible)
      - age_years is numeric
      - no missing patient_id
      - no duplicate patient_id

    "Outlier" ages are logged as WARNING only (synthetic data),
    they do NOT fail validation.
    """
    table = "dim_patient"
    errors: list[str] = []

    logger.info("Starting validation for %s (rows=%d)", table, len(df))

    try:
        # 1) Required columns
        required_cols = [
            "patient_id",
            "birthdate",
            "age_years",
            "gender",
            "state",
            "healthcare_coverage",
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            _log_error(errors, f"{table}: missing required columns: {missing}")
            # critical: if key columns are missing, we consider validation failed
            logger.error("%s validation FAILED (missing required columns)", table)
            return False

        # 2) Type casting / formatting
        # birthdate -> datetime
        try:
            df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")
        except Exception as e:  # noqa: BLE001
            _log_error(errors, f"{table}: failed to convert birthdate to datetime: {e}")

        # age_years -> numeric
        try:
            df["age_years"] = pd.to_numeric(df["age_years"], errors="coerce")
        except Exception as e:  # noqa: BLE001
            _log_error(errors, f"{table}: failed to convert age_years to numeric: {e}")

        # healthcare_coverage -> numeric (if present)
        if "healthcare_coverage" in df.columns:
            try:
                df["healthcare_coverage"] = pd.to_numeric(
                    df["healthcare_coverage"], errors="coerce"
                )
            except Exception as e:  # noqa: BLE001
                _log_error(
                    errors,
                    f"{table}: failed to convert healthcare_coverage to numeric: {e}",
                )

        # 3) Missingness
        null_patient_ids = df["patient_id"].isna().sum()
        if null_patient_ids > 0:
            _log_error(errors, f"{table}: {null_patient_ids} rows have missing patient_id")

        # 4) Duplicates
        dup_patient_ids = df.duplicated(subset=["patient_id"]).sum()
        if dup_patient_ids > 0:
            _log_error(errors, f"{table}: {dup_patient_ids} duplicate patient_id values found")

        # 5) Outlier / logical checks (WARN ONLY for Synthea)
        if pd.api.types.is_numeric_dtype(df["age_years"]):
            bad_age = (df["age_years"] < 0) | (df["age_years"] > 120)
            if bad_age.any():
                count_bad = bad_age.sum()
                logger.warning(
                    "%s: %d rows have age_years outside [0, 120] (warning only, synthetic data)",
                    table,
                    count_bad,
                )

        # healthcare_coverage should not be negative (treat as error)
        if "healthcare_coverage" in df.columns and pd.api.types.is_numeric_dtype(
            df["healthcare_coverage"]
        ):
            negative_cov = (df["healthcare_coverage"] < 0).sum()
            if negative_cov > 0:
                _log_error(
                    errors,
                    f"{table}: {negative_cov} rows have negative healthcare_coverage",
                )

    except Exception as e:  # any unexpected error
        _log_error(errors, f"{table}: unexpected error during validation: {e}")
        logger.exception("Unexpected error while validating %s", table)

    # Final decision
    if errors:
        logger.error("%s validation FAILED with %d issue(s)", table, len(errors))
        return False

    logger.info("%s validation PASSED", table)
    return True


# ------------------ Encounter fact ------------------

def validate_fact_encounter(df: pd.DataFrame) -> bool:
    """
    Basic validation for the cleaned encounter table (fact_encounter).

    Checks:
      - required columns exist
      - start/stop are datetimes (or coercible)
      - length_of_stay_hours is numeric and non-negative
      - no missing encounter_id
      - no duplicate encounter_id
      - logical: stop >= start
    """
    table = "fact_encounter"
    errors: list[str] = []

    logger.info("Starting validation for %s (rows=%d)", table, len(df))

    try:
        # 1) Required columns
        required_cols = [
            "encounter_id",
            "patient_id",
            "start",
            "stop",
            "length_of_stay_hours",
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            _log_error(errors, f"{table}: missing required columns: {missing}")
            logger.error("%s validation FAILED (missing required columns)", table)
            return False

        # 2) Type casting / formatting
        # Synthea often has timezone-aware datetimes, so we coerce with utc=True
        try:
            df["start"] = pd.to_datetime(df["start"], errors="coerce", utc=True)
            df["stop"] = pd.to_datetime(df["stop"], errors="coerce", utc=True)
        except Exception as e:  # noqa: BLE001
            _log_error(errors, f"{table}: failed to convert start/stop to datetime: {e}")

        try:
            df["length_of_stay_hours"] = pd.to_numeric(
                df["length_of_stay_hours"], errors="coerce"
            )
        except Exception as e:  # noqa: BLE001
            _log_error(
                errors,
                f"{table}: failed to convert length_of_stay_hours to numeric: {e}",
            )

        # 3) Missingness
        null_enc_ids = df["encounter_id"].isna().sum()
        if null_enc_ids > 0:
            _log_error(errors, f"{table}: {null_enc_ids} rows have missing encounter_id")

        # 4) Duplicates
        dup_enc_ids = df.duplicated(subset=["encounter_id"]).sum()
        if dup_enc_ids > 0:
            _log_error(errors, f"{table}: {dup_enc_ids} duplicate encounter_id values found")

        # 5) Logical checks
        # stop >= start (we assume both are datetime-like after coercion)
        try:
            invalid_time = (df["stop"] < df["start"]).sum()
            if invalid_time > 0:
                _log_error(errors, f"{table}: {invalid_time} rows have stop < start")
        except Exception as e:  # if comparison fails for some reason
            _log_error(errors, f"{table}: error comparing start/stop: {e}")

        # length_of_stay_hours should not be negative
        if pd.api.types.is_numeric_dtype(df["length_of_stay_hours"]):
            negative_los = (df["length_of_stay_hours"] < 0).sum()
            if negative_los > 0:
                _log_error(
                    errors,
                    f"{table}: {negative_los} rows have negative length_of_stay_hours",
                )

    except Exception as e:
        _log_error(errors, f"{table}: unexpected error during validation: {e}")
        logger.exception("Unexpected error while validating %s", table)

    if errors:
        logger.error("%s validation FAILED with %d issue(s)", table, len(errors))
        return False

    logger.info("%s validation PASSED", table)
    return True
