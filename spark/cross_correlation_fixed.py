from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when, round

spark = SparkSession.builder.appName("CrossCorrelation").getOrCreate()

# Load Crime data
crime_df = spark.read.option("header", "true").csv("../data/Crimes_Sample_50k_clean.csv")
for old in crime_df.columns:
    crime_df = crime_df.withColumnRenamed(old, old.replace(" ", "_"))

print("=== CORRELATION 1: Crime Rate vs Arrest Rate by District ===")
crime_by_district = crime_df.groupBy("District").agg(
    count("*").alias("total_crimes"),
    sum(when(crime_df["Arrest"] == "true", 1).otherwise(0)).alias("total_arrests")
).withColumn("arrest_rate", round((col("total_arrests") / col("total_crimes")) * 100, 2))

crime_by_district.orderBy(col("arrest_rate").desc()).show(15)

# Load Violence data
violence_df = spark.read.option("header", "true").csv("../data/Violence_Reduction.csv")

print("\n=== CORRELATION 2: Crime vs Violence by District ===")
violence_cols = violence_df.columns
print("Violence columns:", violence_cols[:5] if violence_cols else "No columns found")

if "DISTRICT" in violence_cols:
    violence_by_district = violence_df.groupBy("DISTRICT").agg(
        count("*").alias("violence_count")
    ).withColumnRenamed("DISTRICT", "District")
    
    crime_violence = crime_by_district.join(violence_by_district, "District", "inner")
    crime_violence.select("District", "total_crimes", "violence_count", "arrest_rate").orderBy(col("total_crimes").desc()).show(15)
else:
    print("DISTRICT column not found - showing crime data only")
    crime_by_district.select("District", "total_crimes", "arrest_rate").orderBy(col("total_crimes").desc()).show(15)

print("\n=== CORRELATION 3: Pearson Correlation ===")
correlation = crime_by_district.stat.corr("total_crimes", "total_arrests")
print(f"Pearson correlation between total crimes and total arrests: {correlation:.4f}")

print("\n✅ Cross-correlations completed!")
spark.stop()