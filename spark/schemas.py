from __future__ import annotations

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

import csv
from pathlib import Path


def string_schema_from_csv_header(csv_path: str) -> StructType:
    """
    Build an explicit StructType schema (all StringType) from the CSV header row.

    This enforces schema (no type inference) while matching real-world exports whose
    columns may change / include many fields.
    """
    p = Path(csv_path)
    with p.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    return StructType([StructField(h, StringType(), True) for h in header])


def crime_schema() -> StructType:
    # Minimal + common columns used across analytics; keep as strings where source is dirty.
    return StructType(
        [
            StructField("Case Number", StringType(), True),
            StructField("Date", StringType(), True),
            StructField("Block", StringType(), True),
            StructField("Primary Type", StringType(), True),
            StructField("District", StringType(), True),
            StructField("Arrest", StringType(), True),
            StructField("Latitude", StringType(), True),
            StructField("Longitude", StringType(), True),
            StructField("Community Area", StringType(), True),
            StructField("Ward", StringType(), True),
        ]
    )


def arrests_schema() -> StructType:
    return StructType(
        [
            StructField("CB_NO", StringType(), True),
            StructField("CASE_NUMBER", StringType(), True),
            StructField("ARREST_DATE", StringType(), True),
            StructField("RACE", StringType(), True),
            StructField("CHARGE_1_STATUTE", StringType(), True),
            StructField("CHARGE_1_DESCRIPTION", StringType(), True),
            StructField("CHARGE_1_TYPE", StringType(), True),
            StructField("CHARGE_1_CLASS", StringType(), True),
        ]
    )


def police_stations_schema() -> StructType:
    return StructType(
        [
            StructField("DISTRICT", StringType(), True),
            StructField("DISTRICT_NAME", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("ZIP", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("LATITUDE", StringType(), True),
            StructField("LONGITUDE", StringType(), True),
        ]
    )


def violence_schema() -> StructType:
    return StructType(
        [
            StructField("CASE_NUMBER", StringType(), True),
            StructField("DATE", StringType(), True),
            StructField("BLOCK", StringType(), True),
            StructField("MONTH", StringType(), True),
            StructField("DISTRICT", StringType(), True),
            StructField("COMMUNITY_AREA", StringType(), True),
            StructField("HOMICIDE_VICTIM", StringType(), True),
            StructField("GUNSHOT_INJURY_I", StringType(), True),
        ]
    )


def sex_offenders_schema() -> StructType:
    return StructType(
        [
            StructField("FIRST", StringType(), True),
            StructField("LAST", StringType(), True),
            StructField("BLOCK", StringType(), True),
            StructField("GENDER", StringType(), True),
            StructField("RACE", StringType(), True),
            StructField("BIRTH_DATE", StringType(), True),
            StructField("VICTIM_MINOR", StringType(), True),
            StructField("DISTRICT", StringType(), True),
        ]
    )

