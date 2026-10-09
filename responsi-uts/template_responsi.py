"""
================================================================================
SCRIPT TEMPLATE KILAT RESPONSI UTS - PRAKTIKUM BIG DATA
================================================================================
File ini berisi seluruh template kode PySpark siap pakai yang paling sering
keluar dalam ujian praktikum responsi. 

Kamu bisa:
1. Menjalankan file ini langsung: `uv run python responsi-uts/template_responsi.py`
2. Atau meng-copy potongan kode di bawah ini ke sel Jupyter Notebook saat ujian!
================================================================================
"""

import os
import sys
import getpass

# ------------------------------------------------------------------------------
# 1. SETUP ISOLASI ENVIRONMENT (ARCH LINUX / LINUX ZERO-CONFLICT)
# ------------------------------------------------------------------------------
# Wajib dipanggil sebelum mengimpor atau membuat SparkSession
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_spark_session(app_name="Responsi-UTS"):
    """Fungsi pembantu membuat SparkSession bersih dengan log level ERROR."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print(f"✅ SparkSession [{app_name}] berhasil aktif!")
    return spark


# ------------------------------------------------------------------------------
# 2. CONTOH IMPLEMENTASI LENGKAP END-TO-END PIPELINE
# ------------------------------------------------------------------------------
def demo_pipeline_responsi():
    spark = get_spark_session("DemoPipelineResponsi")
    username = os.environ.get("USER", getpass.getuser())

    print("\n--- [A. EXTRACT] Membaca Data ---")
    # Path HDFS standar klaster praktikum
    hdfs_base = f"hdfs://localhost:9000/user/{username}"

    # Template membaca CSV
    # df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{hdfs_base}/transaksi.csv")

    # Template membaca JSON
    # df_json = spark.read.json(f"{hdfs_base}/master_toko.json")

    # Demo dataset dummy jika dijalankan tanpa HDFS
    dummy_data = [
        ("TRX-001", "Magelang", "Elektronik", 2, 500000, 5),
        ("TRX-002", "Magelang", "Fashion", 5, 100000, None),
        ("TRX-003", "Yogyakarta", "Elektronik", 1, 1500000, 4),
        ("TRX-004", "Yogyakarta", "Makanan", 10, 25000, 5),
        ("TRX-005", "Semarang", "Elektronik", 3, 600000, None),
        ("TRX-006", "Semarang", "Fashion", 4, 150000, 3),
        ("TRX-007", "Purworejo", "Makanan", 20, 15000, 4),
        ("TRX-008", "Purworejo", "Elektronik", 1, 2000000, None),
    ]
    columns = ["order_id", "kota", "kategori", "unit_terjual", "harga_satuan", "rating"]
    df = spark.createDataFrame(dummy_data, schema=columns)
    print("Data awal berhasil dimuat:")
    df.show()

    print("\n--- [B. TRANSFORM: Kolom, Filter, & Handling Null] ---")
    # 1. Tambah kolom total pendapatan
    df_transformed = df.withColumn(
        "total_pendapatan", F.col("unit_terjual") * F.col("harga_satuan")
    )

    # 2. Flag boolean apakah ada rating ulasan
    df_transformed = df_transformed.withColumn("ada_rating", F.col("rating").isNotNull())

    # 3. Imputasi null dengan nilai default (sentinel value)
    df_transformed = df_transformed.na.fill({"rating": 0})

    # 4. Filter baris dengan kondisi ganda
    df_filter = df_transformed.filter(
        (F.col("total_pendapatan") >= 500000) & (F.col("kota") != "Purworejo")
    )
    print("Hasil filter (omset >= 500rb dan bukan Purworejo):")
    df_filter.show()

    print("\n--- [C. TRANSFORM: Relational Join] ---")
    # Data master cabang
    cabang_data = [
        ("Magelang", "Budi Santoso", 3000000),
        ("Yogyakarta", "Siti Aminah", 5000000),
        ("Semarang", "Agus Setiawan", 4500000),
        ("Purworejo", "Dewi Lestari", 2500000),
    ]
    df_cabang = spark.createDataFrame(cabang_data, ["kota", "pic_cabang", "target_omset"])

    # Best practice: Agregasi transaksi terlebih dahulu sebelum join
    df_omset_kota = df_transformed.groupBy("kota").agg(
        F.sum("total_pendapatan").alias("realisasi_omset")
    )

    # Left join dengan tabel cabang
    df_pencapaian = df_omset_kota.join(df_cabang, on="kota", how="inner") \
        .withColumn("persen_capaian", F.round((F.col("realisasi_omset") / F.col("target_omset")) * 100, 2)) \
        .orderBy(F.col("persen_capaian").desc())

    print("Hasil Join & Analisis Pencapaian Target Cabang:")
    df_pencapaian.show()

    print("\n--- [D. TRANSFORM: Window Function (Top-1 Kategori per Kota)] ---")
    # Agregasi omset per kota dan kategori
    df_kota_kat = df_transformed.groupBy("kota", "kategori").agg(
        F.sum("total_pendapatan").alias("omset_kategori")
    )

    # Window spesifikasi per kota urut omset menurun
    window_spec = Window.partitionBy("kota").orderBy(F.col("omset_kategori").desc())

    # Ambil peringkat 1 menggunakan row_number()
    df_top_kategori = df_kota_kat \
        .withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") == 1) \
        .orderBy(F.col("omset_kategori").desc())

    print("Kategori Terlaris (Juara 1) di Masing-masing Kota:")
    df_top_kategori.show()

    print("\n--- [E. TRANSFORM: Spark SQL] ---")
    # Daftarkan DataFrame sebagai Temporary View
    df_transformed.createOrReplaceTempView("transaksi")
    df_cabang.createOrReplaceTempView("cabang")

    df_sql = spark.sql("""
        SELECT 
            t.kota,
            c.pic_cabang,
            COUNT(t.order_id) AS total_transaksi,
            SUM(t.total_pendapatan) AS total_penjualan
        FROM transaksi t
        JOIN cabang c ON t.kota = c.kota
        GROUP BY t.kota, c.pic_cabang
        ORDER BY total_penjualan DESC
    """)
    print("Hasil Kueri Spark SQL:")
    df_sql.show()

    print("\n--- [F. LOAD: Menyimpan Data ke Parquet Terpartisi] ---")
    # Simpan ke folder lokal/HDFS
    output_target = "output_demo_parquet"
    df_transformed.write.mode("overwrite") \
        .partitionBy("kategori") \
        .parquet(output_target)
    print(f"✅ Data berhasil disimpan ke: {output_target}")

    # Verifikasi baca ulang
    df_cek = spark.read.parquet(output_target)
    print(f"Verifikasi pembacaan ulang: {df_cek.count()} baris terbaca utuh.")

    # Bersihkan sesi Spark
    spark.stop()
    print("\n🛑 SparkSession selesai dan telah ditutup.")


if __name__ == "__main__":
    demo_pipeline_responsi()
