from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TrainKMeansModel") \
    .getOrCreate()

# Create dummy historical features: [z_score, frequency_count, gas_fee]
data = [
    (0.1, 1, 10.0), (0.5, 2, 12.0), (-0.2, 1, 11.0),  # Normal
    (4.5, 12, 150.0), (3.8, 10, 200.0), (5.1, 15, 180.0) # Anomaly / Burst
]
columns = ["z_score", "tx_count", "gas_fee"]
df = spark.createDataFrame(data, columns)

# Assemble features into a vector column
assembler = VectorAssembler(inputCols=["z_score", "tx_count", "gas_fee"], outputCol="features")

# Define KMeans (2 clusters: Normal vs Anomaly)
kmeans = KMeans(k=2, seed=42, featuresCol="features", predictionCol="cluster_label")

# Build and fit pipeline
pipeline = Pipeline(stages=[assembler, kmeans])
model = pipeline.fit(df)

# Save model to disk
model.write().overwrite().save("kmeans_model")
print("KMeans model trained and saved successfully to 'kmeans_model/'")

spark.stop()
