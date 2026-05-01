from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import *
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.runtime import load_config, resolve_data_path

spark = SparkSession.builder.appName("CrimeHotspots").getOrCreate()

# Load data
CFG = load_config()
crime_path = str(resolve_data_path(CFG, "crime"))
df = spark.read.option("header", "true").csv(crime_path)

# Clean column names
for old_name in df.columns:
    new_name = old_name.replace(" ", "_")
    df = df.withColumnRenamed(old_name, new_name)

# Filter valid coordinates
hotspot_df = df.filter(
    (col("Latitude") != "") & 
    (col("Longitude") != "") &
    col("Latitude").isNotNull() &
    col("Longitude").isNotNull()
).select(
    col("Latitude").cast("double"),
    col("Longitude").cast("double")
).dropna()

print(f"Crimes with valid coordinates: {hotspot_df.count()}")

# Assemble features
assembler = VectorAssembler(inputCols=["Latitude", "Longitude"], outputCol="features")
assembled_df = assembler.transform(hotspot_df)

# K-Means (default k=10 per assignment; can change in code if needed)
kmeans = KMeans().setK(10).setSeed(42).setFeaturesCol("features").setPredictionCol("cluster")
model = kmeans.fit(assembled_df)

# Show cluster centers
print("\n=== Crime Hotspot Centers (Latitude, Longitude) ===")
for i, center in enumerate(model.clusterCenters()):
    print(f"Hotspot {i+1}: ({center[0]:.6f}, {center[1]:.6f})")