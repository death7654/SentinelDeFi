# SentinelDeFi

A real-time DeFi transaction processing and anomaly detection engine powered by **PySpark Structured Streaming** and **Apache Kafka**.

---

## Prerequisites & Setup

Before starting the pipeline, ensure the following components are installed and configured on your Windows machine:

### 1. Java Development Kit (JDK)
* **JDK 17** or **JDK 21** installed and configured in your environment variables.
* Verify with:
  ```powershell
  java -version

```

### 2. Hadoop Binaries (`winutils` & `hadoop.dll`)

PySpark on Windows requires native Hadoop binaries to handle filesystem checkpoints.

1. Download Hadoop binaries (Hadoop 3.x) and place them in `C:\hadoop\bin`.
2. Ensure `C:\hadoop\bin\winutils.exe` and `C:\hadoop\bin\hadoop.dll` exist.
3. Set your environment variables:
```powershell
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[System.Environment]::SetEnvironmentVariable("Path", $env:Path + ";C:\hadoop\bin", "User")

```



---

## Python Dependencies

Install the required Python packages inside your virtual environment:

```powershell
pip install pyspark kafka-python

```

*(Note: The `spark-sql-kafka` connector JAR is dynamically downloaded by PySpark at runtime matching your exact PySpark version).*

---

## How to Start the Entire Pipeline

Follow these steps in order across separate PowerShell windows.

### Step 1: Start Apache Kafka

Ensure your local Kafka broker is up and running on port `9092`.

* **KRaft Mode (Kafka 3.x+):**
```powershell
.\bin\windows\kafka-server-start.bat .\config\kraft\server.properties

```



---

### Step 2: Create the Kafka Topic

Create the `defi-transactions` topic (if not already created):

```powershell
.\bin\windows\kafka-topics.bat --create --topic defi-transactions --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1

```

To inspect partition offsets and active traffic:

```powershell
.\bin\windows\kafka-topics.bat --describe --topic defi-transactions --bootstrap-server localhost:9092

```

---

### Step 3: Run the Transaction Producer

Start your Kafka producer script to simulate incoming DeFi transaction streams:

```powershell
python producer.py

```

*Expected output:*

```text
[+] Emitted tx: 0xcb70fb75... | Wallet: 0x0000...0005 | Amount: $715.52
[+] Emitted tx: 0x1309e915... | Wallet: 0x0000...0004 | Amount: $421.71

```

---

### Step 4: Run the PySpark Streaming Engine

In a new terminal window, start the streaming analysis pipeline:

```powershell
python streaming_engine.py

```

*Expected output:*

```text
PySpark 4.2.0 Structured Streaming Engine Started with org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0...

-------------------------------------------
Batch: 0
-------------------------------------------
+----------------------------------+----------------------------------+----------+-------+--------------------------+
|tx_id                             |wallet_address                    |amount_usd|gas_fee|timestamp                 |
+----------------------------------+----------------------------------+----------+-------+--------------------------+
|0xcb70fb75a4a9a2c3f829d733ea556de...|0x00000000000000000000000000000005|715.52    |0.0118 |2026-08-02 19:08:03.810916|
+----------------------------------+----------------------------------+----------+-------+--------------------------+

```

---

## Streaming Data Schema

Incoming JSON byte payloads are deserialized using the following Spark schema:

| Column Field | Data Type | Description |
| --- | --- | --- |
| `tx_id` | `StringType` | Hexadecimal transaction hash identifier |
| `wallet_address` | `StringType` | Sender wallet address |
| `amount_usd` | `DoubleType` | Transaction value in USD |
| `gas_fee` | `DoubleType` | Network execution gas fee |
| `timestamp` | `TimestampType` | Event execution time |

---

## Common Troubleshooting

### 1. `java.lang.UnsatisfiedLinkError: NativeIO$Windows.access0`

* **Cause:** Spark cannot link to `hadoop.dll`.
* **Fix:** Ensure `os.environ["HADOOP_HOME"] = r"C:\hadoop"` and `os.environ["PATH"] += r";C:\hadoop\bin"` are declared at the very top of `streaming_engine.py` before `SparkSession` is created.

### 2. `java.lang.NoSuchMethodError: SerializedOffset.<init>`

* **Cause:** Mismatch between installed `pyspark` package version and the requested Maven package (`spark-sql-kafka`).
* **Fix:** `streaming_engine.py` dynamically resolves the artifact version using `pyspark.__version__` (`org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`). Ensure you do not hardcode older versions like `3.5.0` or `4.0.0`.

```