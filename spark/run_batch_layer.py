from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    date_format,
    lit,
    month,
    round,
    sum as fsum,
    to_json,
    to_timestamp,
    when,
    year,
    hour as fhour,
    row_number,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from config.runtime import get_postgres, load_config, resolve_data_path
from schemas import string_schema_from_csv_header


def _read_csv(spark: SparkSession, path: str, schema):
    return (
        spark.read.option("header", "true")
        .schema(schema)
        .csv(path)
    )


def _rename_spaces(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.replace(" ", "_"))
    return df


def _jdbc_write(df, pg, table: str):
    (
        df.write.format("jdbc")
        .mode("overwrite")
        .options(
            url=f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.database}",
            driver="org.postgresql.Driver",
            dbtable=table,
            user=pg.user,
            password=pg.password,
        )
        .save()
    )


def _pick_col(df, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _pick_col_prefix(df, prefixes: list[str]) -> str | None:
    for c in df.columns:
        for p in prefixes:
            if c.startswith(p):
                return c
    return None


def main() -> int:
    cfg = load_config()
    pg = get_postgres(cfg)

    spark = SparkSession.builder.appName("CrimeAnalyticsBatchLayer").getOrCreate()

    # Load datasets with explicit schemas (all-string schema derived from each file's header)
    crime_path = str(resolve_data_path(cfg, "crime"))
    arrests_path = str(resolve_data_path(cfg, "arrests"))
    police_path = str(resolve_data_path(cfg, "police_stations"))
    violence_path = str(resolve_data_path(cfg, "violence"))
    sex_path = str(resolve_data_path(cfg, "sex_offenders"))

    crime = _rename_spaces(_read_csv(spark, crime_path, string_schema_from_csv_header(crime_path)))
    arrests = _rename_spaces(_read_csv(spark, arrests_path, string_schema_from_csv_header(arrests_path)))
    police = _rename_spaces(_read_csv(spark, police_path, string_schema_from_csv_header(police_path)))
    violence = _rename_spaces(_read_csv(spark, violence_path, string_schema_from_csv_header(violence_path)))
    sex = _rename_spaces(_read_csv(spark, sex_path, string_schema_from_csv_header(sex_path)))

    # Normalize key fields (handle common naming variants after _rename_spaces)
    if "Case_Number" not in crime.columns and "Case_Number" in [c.replace(" ", "_") for c in crime.columns]:
        pass
    if "Case_Number" not in crime.columns and "Case_Number" not in crime.columns:
        # Some exports use "Case_Number" already; others use "Case_Number" after rename; keep as-is.
        pass

    # Crimes export usually includes "Case_Number"
    crime = crime.withColumn("Case_Number", col("Case_Number").cast("string"))

    # Arrests export may include "CASE_NUMBER" or "CASE_NUMBER"
    if "CASE_NUMBER" not in arrests.columns and "CASE_NUMBER" in [c.upper() for c in arrests.columns]:
        # find the actual column name matching case_number
        for c in arrests.columns:
            if c.upper() == "CASE_NUMBER":
                arrests = arrests.withColumnRenamed(c, "CASE_NUMBER")
                break
    arrests = arrests.withColumn("CASE_NUMBER", col("CASE_NUMBER").cast("string"))

    # Parse crime timestamp (tolerant parsing; invalid timestamps -> NULL)
    crime = crime.withColumn("crime_ts", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))

    # 7.1 Crime Trend Analysis (year, month, day_of_week, hour)
    trends = (
        crime.filter(col("crime_ts").isNotNull())
        .withColumn("year", year("crime_ts"))
        .withColumn("month", month("crime_ts"))
        .withColumn("day_of_week", date_format(col("crime_ts"), "EEEE"))
        .withColumn("hour", fhour("crime_ts"))
        .groupBy("year", "month", "day_of_week", "hour")
        .agg(count(lit(1)).alias("crime_count"))
    )
    _jdbc_write(trends, pg, "crime_trends")

    # 7.2 Arrest Rate Analysis (join crimes ↔ arrests on CASE NUMBER)
    # Crime has "Arrest" but spec wants join; use arrests table existence as numerator.
    joined = crime.join(arrests, crime.Case_Number == arrests.CASE_NUMBER, "left")
    joined = joined.withColumn("has_arrest_record", when(col("CB_NO").isNotNull(), lit(1)).otherwise(lit(0)))

    arrest_rates = (
        joined.withColumn("District_int", col("District").cast("int"))
        .groupBy("Primary_Type", "District_int", "RACE")
        .agg(
            count(lit(1)).alias("total_crimes"),
            fsum("has_arrest_record").alias("total_arrests"),
        )
        .withColumn("arrest_rate", round((col("total_arrests") / col("total_crimes")) * 100, 2))
        .withColumnRenamed("Primary_Type", "crime_type")
        .withColumnRenamed("District_int", "district")
        .withColumnRenamed("RACE", "race")
        .select("crime_type", "district", "race", "arrest_rate")
    )
    _jdbc_write(arrest_rates, pg, "arrest_rates")

    # 7.3 Violence and Gunshot Analysis
    violence = violence.withColumn("month_int", col("MONTH").cast("int")).withColumn("district_int", col("DISTRICT").cast("int"))

    homicide_col = _pick_col(violence, ["HOMICIDE_VICTIM"]) or _pick_col_prefix(violence, ["HOMICIDE_VICTIM"])
    gunshot_col = _pick_col(violence, ["GUNSHOT_INJURY_I"]) or _pick_col_prefix(violence, ["GUNSHOT_INJURY"])

    if homicide_col is None:
        raise RuntimeError("Could not find a homicide victim column in violence dataset (expected HOMICIDE_VICTIM*).")
    if gunshot_col is None:
        raise RuntimeError("Could not find a gunshot injury column in violence dataset (expected GUNSHOT_INJURY*).")

    hom = fsum(when(col(homicide_col) == "Y", 1).otherwise(0)).alias("homicides_count")
    nonfatal = fsum(when(col(homicide_col) == "Y", 0).otherwise(1)).alias("nonfatal_shootings_count")
    gunshot_rate = round(
        (fsum(when(col(gunshot_col) == "Y", 1).otherwise(0)) / count(lit(1))),
        4,
    ).alias("gunshot_injury_rate")

    violence_month_district = (
        violence.filter(col("month_int").isNotNull() & col("district_int").isNotNull())
        .groupBy("month_int", "district_int")
        .agg(hom, nonfatal, gunshot_rate, count(lit(1)).alias("total_incidents"))
    )

    # Top community area by incidents (overall) for simple reporting in same row
    ca_counts = (
        violence.filter(col("COMMUNITY_AREA").isNotNull())
        .withColumn(
            "community_area_int",
            when(col("COMMUNITY_AREA").rlike("^[0-9]+$"), col("COMMUNITY_AREA").cast("int")).otherwise(lit(None)),
        )
        .filter(col("community_area_int").isNotNull())
        .groupBy("community_area_int")
        .agg(count(lit(1)).alias("cnt"))
    )
    w = Window.orderBy(col("cnt").desc())
    top_ca = ca_counts.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).select(
        col("community_area_int").alias("top_community_area"),
        col("cnt").alias("top_community_area_count"),
    )

    violence_stats = (
        violence_month_district.crossJoin(top_ca)
        .select(
            col("month_int").alias("month"),
            col("district_int").alias("district"),
            "homicides_count",
            "nonfatal_shootings_count",
            "gunshot_injury_rate",
            "top_community_area",
            "top_community_area_count",
        )
    )
    _jdbc_write(violence_stats, pg, "violence_stats")

    # 7.4 Sex Offender Proximity / density by district (and victim_minor flag)
    victim_minor_col = _pick_col(sex, ["VICTIM_MINOR"]) or _pick_col_prefix(sex, ["VICTIM_MINOR", "VICTIM"])
    if victim_minor_col is None:
        # If column doesn't exist in the export, treat as all non-minor (still produces density by district).
        sex = sex.withColumn("VICTIM_MINOR", lit(None))
        victim_minor_col = "VICTIM_MINOR"

    if "DISTRICT" not in sex.columns:
        # Some exports don't include district; keep a single aggregated row with district NULL.
        sex = sex.withColumn("DISTRICT", lit(None))

    sex_density = (
        sex.withColumn("district_int", col("DISTRICT").cast("int"))
        .groupBy("district_int")
        .agg(
            count(lit(1)).alias("offender_count"),
            fsum(when(col(victim_minor_col) == "Y", 1).otherwise(0)).alias("victim_minor_count"),
        )
        .select(col("district_int").alias("district"), "offender_count", "victim_minor_count")
    )
    _jdbc_write(sex_density, pg, "sex_offender_density")

    # 7.5 Geospatial Hotspot Detection (K-Means k=10) and store centroids
    geo = (
        crime.select(col("Latitude").cast("double").alias("latitude"), col("Longitude").cast("double").alias("longitude"))
        .dropna()
    )
    assembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
    geo2 = assembler.transform(geo)
    model = KMeans(k=10, seed=42, featuresCol="features", predictionCol="cluster_id").fit(geo2)
    clustered = model.transform(geo2)

    centers = spark.createDataFrame(
        [(i, float(c[0]), float(c[1])) for i, c in enumerate(model.clusterCenters())],
        ["cluster_id", "latitude", "longitude"],
    )
    counts = clustered.groupBy("cluster_id").agg(count(lit(1)).alias("crime_count"))
    hotspots = centers.join(counts, "cluster_id", "left").fillna({"crime_count": 0})
    _jdbc_write(hotspots, pg, "hotspots")

    # 7.6 Cross-Dataset Correlation (store at least 2)
    # (a) Violence rate vs arrest rate by district: correlate violence_count with arrest_rate
    violence_by_district = violence.groupBy(col("DISTRICT").cast("int").alias("district")).agg(count(lit(1)).alias("violence_count"))
    arrest_by_district2 = (
        joined.withColumn("district", col("District").cast("int"))
        .groupBy("district")
        .agg(
            count(lit(1)).alias("crime_count"),
            fsum("has_arrest_record").alias("arrest_count"),
        )
        .withColumn("arrest_rate", (col("arrest_count") / col("crime_count")) * 100)
    )
    dv = arrest_by_district2.join(violence_by_district, "district", "inner")
    corr_val1 = dv.stat.corr("violence_count", "arrest_rate")

    # (b) Sex offender density vs crime rate by district: correlate offender_count with crime_count
    sd = sex_density.select("district", col("offender_count").cast("double").alias("offender_count"))
    dc = arrest_by_district2.select("district", col("crime_count").cast("double").alias("crime_count"))
    d2 = dc.join(sd, "district", "inner")
    corr_val2 = d2.stat.corr("offender_count", "crime_count")

    corr_rows = [
        ("violence_vs_arrest_rate_by_district", None, float(corr_val1), '{"note":"pearson(violence_count, arrest_rate)"}'),
        ("sex_offender_density_vs_crime_count_by_district", None, float(corr_val2), '{"note":"pearson(offender_count, crime_count)"}'),
    ]
    corr_schema = StructType(
        [
            StructField("correlation_type", StringType(), False),
            StructField("district", IntegerType(), True),
            StructField("value", DoubleType(), True),
            StructField("metadata", StringType(), True),
        ]
    )
    corr_df = spark.createDataFrame(corr_rows, schema=corr_schema)
    _jdbc_write(corr_df, pg, "correlations")

    spark.stop()
    print("✅ Batch layer complete: crime_trends, arrest_rates, violence_stats, sex_offender_density, hotspots, correlations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

