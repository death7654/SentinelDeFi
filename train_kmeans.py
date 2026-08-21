import os
import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

# 1. Force PySpark workers to use the exact active Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 2. Windows Native Hadoop Configuration (prevents winutils warning)
os.makedirs(r"C:\hadoop\logs", exist_ok=True)
os.environ["HADOOP_HOME"] = r"C:\hadoop"
if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
    os.environ["PATH"] += r";C:\hadoop\bin"

# Force path to save model directly inside project directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(SCRIPT_DIR, "kmeans_model")

# 3. Build Spark Session
spark = SparkSession.builder \
    .appName("TrainKMeansModel") \
    .config("spark.driver.extraJavaOptions", "-Dderby.stream.error.file=C:/hadoop/logs/derby.log") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Dummy historical metrics: [z_score, frequency_count, gas_fee]
data = [
    (0.1, 1, 10.0), (0.5, 2, 12.0), (-0.2, 1, 11.0),   # Normal transactions
    (4.5, 12, 150.0), (3.8, 10, 200.0), (5.1, 15, 180.0) # Anomaly / Bot Bursts
]
columns = ["z_score", "tx_count", "gas_fee"]
df = spark.createDataFrame(data, columns)

# Assemble feature vectors
assembler = VectorAssembler(inputCols=["z_score", "tx_count", "gas_fee"], outputCol="features")
kmeans = KMeans(k=2, seed=42, featuresCol="features", predictionCol="prediction")
pipeline = Pipeline(stages=[assembler, kmeans])

# Train and save pipeline model
model = pipeline.fit(df)
model.write().overwrite().save(MODEL_PATH)

print(f"\nSUCCESS: KMeans model successfully saved to: {MODEL_PATH}\n")

spark.stop()