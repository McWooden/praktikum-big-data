import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Konfigurasi runtime native Arch Linux (Zero-Conflict Isolated)
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 2. Inisialisasi SparkSession otomatis
spark = SparkSession.builder \
    .appName("Praktikum-Big-Data") \
    .master("local[*]") \
    .getOrCreate()

# 3. Redam spam log INFO/WARN agar output notebook bersih
spark.sparkContext.setLogLevel("ERROR")

print(f"⚡ SparkSession siap! (Spark {spark.version} | local[*])")

__all__ = ["spark", "F", "Window"]
