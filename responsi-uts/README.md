# ⚡ PETA KILAT & MASTER CHEATSHEET RESPONSI UTS PRAKTIKUM BIG DATA
> **Panduan Selamat Ujian:** Dibuat khusus dengan bahasa yang sederhana, manusiawi, dan langsung ke inti agar kamu yang merasa "tidak tahu apa-apa" bisa langsung paham benang merah 6 pertemuan dalam hitungan menit dan siap tempur di responsi UTS!

---

## 🎯 Navigasi Berkas di Folder Ini

Jika waktu belajarmu sangat mepet, baca sesuai urutan prioritas ini:

1. **[README.md](./README.md)** *(Halaman Ini)*: Peta konsep besar, alur kerja ujian praktikum, cheat sheet kode siap contek, dan jawaban emas ujian lisan.
2. **[template_responsi.ipynb](./template_responsi.ipynb)** / **[template_responsi.py](./template_responsi.py)**: File template kode super lengkap siap copy-paste saat ngerjain soal live-coding.
3. **Notebook Interaktif Per Pertemuan (Bisa langsung dijalankan sel per sel di Jupyter):**
   - **[01. Pertemuan 02 — Linux, Python & Pandas](./01_pertemuan-02_dasar_linux_python_pandas.ipynb)**: Uji coba Linux CLI, Pandas dasar, dan simulasi limit memori RAM.
   - **[02. Pertemuan 03 — Apache Hadoop & HDFS](./02_pertemuan-03_arsitektur_hadoop_dan_hdfs.ipynb)**: Verifikasi daemon klaster (`jps`), upload berkas ke HDFS, `-ls`, `-cat`, `-du`, dan `-get`.
   - **[03. Pertemuan 04 — Apache Spark & PySpark Dasar](./03_pertemuan-04_dasar_apache_spark_pyspark.ipynb)**: SparkSession, Lazy Evaluation, `select`, `filter`, `withColumn`, dan agregasi.
   - **[04. Pertemuan 05 — Join, Window Function & Spark SQL](./04_pertemuan-05_join_window_functions_sparksql.ipynb)**: Multi-table join, Window Function (`row_number` vs `rank`), dan Spark SQL murni.
   - **[05. Pertemuan 06 — Parquet, JSON & Pipeline ETL](./05_pertemuan-06_format_data_parquet_json_etl.ipynb)**: Format Parquet vs JSON, Column Pruning, Partitioning, dan alur ETL.
   - **[06. Pertemuan 07 — Studi Kasus Terintegrasi End-to-End](./06_pertemuan-07_studi_kasus_terintegrasi.ipynb)**: Simulasi kasus nyata retail triwulan dari ingest HDFS sampai executive summary.

---

## 🧠 The Big Picture: Cerita Sederhana 6 Pertemuan (Biar Langsung "Klik")

Bayangkan kamu adalah data engineer di perusahaan e-commerce besar:

```
[Pertemuan 2: Masa Lalu]
Data masih kecil -> Pake Pandas di laptop -> Gampang & cepat.
Tapi begitu data capai 100 GB -> Laptop crash (MemoryError: RAM habis)!
                         │
                         ▼
[Pertemuan 3: Tempat Penyimpanan Data Raksasa (Storage)]
Solusinya: Data dipecah-pecah ke banyak komputer -> Lahirlah APACHE HADOOP (HDFS).
HDFS ibarat gudang raksasa: NameNode = mandor (tahu lokasi barang), DataNode = kuli/rak (nyimpen barang fisik).
                         │
                         ▼
[Pertemuan 4: Otak Komputasi Cepat (Engine)]
Hadoop punya mesin hitung bernama MapReduce, tapi LAMBAT karena bolak-balik nulis ke harddisk.
Lalu datang APACHE SPARK: Menghitung langsung di RAM (In-Memory), 100x lebih kencang!
Kita pakai PySpark DataFrame dengan sifat "Lazy Evaluation" (tidak langsung hitung sebelum diminta aksi).
                         │
                         ▼
[Pertemuan 5: Operasi Pengolahan Canggih]
Data di dunia nyata terpisah-pisah di banyak tabel:
1. Kita gabungkan tabel (JOIN).
2. Kita cari ranking terbaik per kategori (WINDOW FUNCTION).
3. Kita hitung pakai gaya query database biasa (SPARK SQL).
                         │
                         ▼
[Pertemuan 6: Format File Masa Depan & Pipeline ETL]
Simpan data jangan pakai CSV (lambat & boros ruang)!
Pakai format PARQUET (berorientasi kolom, terkompres Snappy otomatis, hemat 80% storage).
Lalu kita rapihkan jadi alur resmi: EXTRACT (baca) -> TRANSFORM (bersihkan & hitung) -> LOAD (simpan).
                         │
                         ▼
[Pertemuan 7: Gladi Bersih Ujian / Studi Kasus Lengkap]
Semua digabung jadi 1 alur utuh: Ingest ke HDFS -> Extract pakai Spark -> Transform (Join + Window + SQL) -> Load ke Parquet HDFS -> Berikan kesimpulan bisnis ke manajemen!
```

---

## 🚀 Alur Sakti Ujian Responsi (Checklist 5 Langkah)

Saat asisten lab atau dosen memberikan soal ujian, **hampir pasti alurnya adalah 5 langkah ini:**

### Langkah 1: Nyalakan Cluster Hadoop di Terminal
Buka terminal laptopmu dan jalankan:
```bash
# 1. Nyalakan SSH service
sudo systemctl start sshd

# 2. Nyalakan HDFS dan YARN
start-dfs.sh
start-yarn.sh

# 3. Cek apakah 5 daemon utama sudah hidup:
jps
# Pastikan muncul: NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
```
> *Dashboard monitoring jika diminta dosen:*
> - HDFS Web UI: `http://localhost:9870`
> - YARN Web UI: `http://localhost:8088`

### Langkah 2: Buka Jupyter Notebook di Workspace Proyek
```bash
cd /mnt/alterra/repos/praktikum-big-data
uv run jupyter notebook
# atau: uv run jupyter lab
```

### Langkah 3: Siapkan / Upload Data Soal ke HDFS
Jika soal meminta membaca data dari HDFS:
```bash
# Buat folder di HDFS
hdfs dfs -mkdir -p /user/$USER/responsi/raw

# Upload dataset CSV / JSON ke HDFS
hdfs dfs -put -f nama_file.csv /user/$USER/responsi/raw/

# Pastikan file ada di HDFS
hdfs dfs -ls -h /user/$USER/responsi/raw/
```

### Langkah 4: Tulis Kode PySpark di Sel Jupyter (Template Ajaib)
Inisialisasi sesi Spark, load data, olah, dan simpan. *(Lihat bagian Master Code Cheatsheet di bawah).*

### Langkah 5: Bersih-bersih Setelah Selesai (Best Practice Lab)
Setelah sesi responsi selesai:
```bash
stop-yarn.sh
stop-dfs.sh
sudo systemctl stop sshd
```

---

## 📋 Master Code Cheatsheet (Copy-Paste Ready)

### 1. Inisialisasi SparkSession (Arch Linux Zero-Conflict Safe)
```python
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Konfigurasi isolasi lingkungan Arch Linux
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Buat SparkSession
spark = SparkSession.builder \
    .appName("Responsi-UTS") \
    .master("local[*]") \
    .getOrCreate()

# Supaya terminal bersih dari spam log info/warning
spark.sparkContext.setLogLevel("ERROR")
print("✅ SparkSession Aktif!")
```

### 2. Membaca Data (EXTRACT)
```python
username = os.environ.get("USER", "kaky")

# Dari HDFS (CSV)
df_trx = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"hdfs://localhost:9000/user/{username}/responsi/raw/transaksi.csv")

# Dari HDFS (JSON)
df_cabang = spark.read.json(f"hdfs://localhost:9000/user/{username}/responsi/raw/cabang.json")

# Dari disk lokal langsung (jika soal tidak wajib HDFS)
# df_trx = spark.read.csv("transaksi.csv", header=True, inferSchema=True)

df_trx.printSchema()
df_trx.show(5)
```

### 3. Operasi Dasar DataFrame (TRANSFORM Bagian 1)
```python
# 1. Pilih kolom tertentu
df_pilih = df_trx.select("order_id", "kota", "unit_terjual", "harga_satuan")

# 2. Filter baris data
df_filter = df_trx.filter(F.col("unit_terjual") > 5)
# Kondisi ganda: pakai tanda & (AND) atau | (OR) dan bungkus tanda kurung!
df_filter2 = df_trx.filter((F.col("kota") == "Magelang") & (F.col("unit_terjual") >= 3))

# 3. Menambah / Memodifikasi Kolom Baru (withColumn)
df_olah = df_trx.withColumn("total_pendapatan", F.col("unit_terjual") * F.col("harga_satuan"))

# 4. Kondisi IF-ELSE di PySpark (when & otherwise)
df_olah = df_olah.withColumn(
    "status_transaksi",
    F.when(F.col("total_pendapatan") >= 1000000, "Transaksi Besar")
     .otherwise("Transaksi Reguler")
)

# 5. Menangani Nilai Kosong (Missing Values / Null)
# Cek apakah ada null
df_olah = df_olah.withColumn("ada_catatan", F.col("catatan").isNotNull())
# Isi null dengan nilai default (misal rating kosong diisi 0)
df_olah = df_olah.na.fill({"rating": 0, "keterangan": "Tidak Ada"})

# 6. Agregasi Standar (groupBy + agg)
df_ringkasan = df_olah.groupBy("kota").agg(
    F.sum("total_pendapatan").alias("total_omset"),
    F.avg("total_pendapatan").alias("rata_rata_omset"),
    F.count("order_id").alias("jumlah_transaksi")
).orderBy(F.col("total_omset").desc())

df_ringkasan.show()
```

### 4. Relational Join (TRANSFORM Bagian 2)
```python
# Inner Join: Hanya mengambil baris yang cocok di kedua tabel
df_gabung = df_trx.join(df_cabang, on="kota", how="inner")

# Left Join: Pertahankan semua baris transaksi meskipun data referensinya tidak ada
# (SANGAT PENTING: Mencegah hilangnya data transaksi saat join ulasan atau target)
df_gabung = df_trx.join(df_ulasan, on="order_id", how="left")

# BEST PRACTICE UJIAN:
# Agregasi tabel besar terlebih dahulu, baru di-join ke tabel referensi kecil!
df_omset_kota = df_trx.groupBy("kota").agg(F.sum("total_pendapatan").alias("realisasi"))
df_performa = df_omset_kota.join(df_target, on="kota", how="inner") \
    .withColumn("persentase_capaian", F.round((F.col("realisasi") / F.col("target_bulanan")) * 100, 2)) \
    .orderBy(F.col("persentase_capaian").desc())

df_performa.show()
```

### 5. Window Function (TRANSFORM Bagian 3: Ranking Top-N)
```python
# Skenario: Cari 1 produk terlaris di setiap kategori / kota
# Langkah A: Agregasi total penjualan per kombinasi kota & produk
df_kota_produk = df_olah.groupBy("kota", "nama_produk").agg(
    F.sum("total_pendapatan").alias("total_penjualan")
)

# Langkah B: Definisikan window partisi dan pengurutan
window_spec = Window.partitionBy("kota").orderBy(F.col("total_penjualan").desc())

# Langkah C: Beri nomor urut (row_number) dan filter juara 1 (peringkat == 1)
df_top1 = df_kota_produk \
    .withColumn("peringkat", F.row_number().over(window_spec)) \
    .filter(F.col("peringkat") == 1) \
    .orderBy(F.col("total_penjualan").desc())

df_top1.show()
```

### 6. Spark SQL (TRANSFORM Bagian 4: Kueri Gaya SQL)
```python
# 1. Daftarkan DataFrame sebagai Temporary View
df_olah.createOrReplaceTempView("transaksi")
df_cabang.createOrReplaceTempView("cabang")

# 2. Jalankan kueri SQL murni
df_sql = spark.sql("""
    SELECT 
        c.kota,
        c.manager,
        COUNT(t.order_id) AS total_transaksi,
        SUM(t.total_pendapatan) AS total_omset
    FROM transaksi t
    INNER JOIN cabang c ON t.kota = c.kota
    GROUP BY c.kota, c.manager
    ORDER BY total_omset DESC
""")

df_sql.show()
```

### 7. Menyimpan Hasil Akhir (LOAD)
```python
# Simpan ke Parquet terpartisi kolom tertentu di HDFS
output_hdfs = f"hdfs://localhost:9000/user/{username}/responsi/processed/laporan_final"

df_olah.write.mode("overwrite") \
    .partitionBy("kota") \
    .parquet(output_hdfs)

print("✅ Data berhasil disimpan ke HDFS dalam format Parquet terpartisi!")

# Verifikasi: Baca ulang dari HDFS untuk memastikan data tidak corrupt
df_verifikasi = spark.read.parquet(output_hdfs)
print(f"Jumlah baris tersimpan: {df_verifikasi.count()}")
```

### 8. Menutup SparkSession (Wajib di Akhir!)
```python
spark.stop()
print("🛑 SparkSession berhasil dihentikan, RAM & CPU bebas.")
```

---

## 💻 Terminal Cheatsheet: Perintah Dasar HDFS Wajib Hafal

| Perintah | Sintaks Lengkap | Penjelasan Sederhana |
|---|---|---|
| **Buat Folder** | `hdfs dfs -mkdir -p /user/$USER/raw` | Membuat folder secara berjenjang di HDFS |
| **Upload File** | `hdfs dfs -put -f file_lokal.csv /user/$USER/raw/` | Mengunggah file dari laptop ke HDFS (`-f` = overwrite jika sudah ada) |
| **Lihat Isi Folder** | `hdfs dfs -ls -h /user/$USER/raw/` | Melihat daftar file di HDFS beserta ukuran terbaca (KB/MB) |
| **Intip Isi File** | `hdfs dfs -cat /user/$USER/raw/file.csv \| head -n 10` | Menampilkan 10 baris pertama file dari HDFS ke terminal |
| **Download File** | `hdfs dfs -get -f /user/$USER/raw/file.csv ./` | Menyalin file dari HDFS turun ke disk laptop |
| **Cek Ukuran Disk** | `hdfs dfs -du -h /user/$USER/` | Melihat berapa megabyte ruang HDFS yang terpakai |
| **Hapus File/Folder** | `hdfs dfs -rm -r /user/$USER/raw/file.csv` | Menghapus file atau direktori di HDFS secara rekursif |

---

## 🎓 Jawaban Emas Pertanyaan Teori & Dosen (Gold Standard Answers)

Jika dosen atau asisten menanyakan teori saat responsi, gunakan jawaban singkat, tegas, dan berbobot berikut:

### Q1: Apa bedanya Apache Hadoop vs Apache Spark?
> **Jawaban:** "Hadoop fokus utama pada penyimpanan file terdistribusi (*HDFS*) dan memiliki mesin proses batch klasik (*MapReduce*) yang lambat karena selalu menulis data perantara ke hard disk. Sedangkan **Apache Spark** adalah engine komputasi modern yang memproses data langsung di dalam memori RAM (*In-Memory Computing*), sehingga eksekusinya bisa 10 hingga 100 kali lebih cepat daripada Hadoop MapReduce."

### Q2: Jelaskan arsitektur HDFS! Apa fungsi NameNode dan DataNode?
> **Jawaban:** "HDFS menggunakan arsitektur Master-Worker:
> - **NameNode (Master):** Mengelola metadata direktori, nama file, hak akses, dan memetakan lokasi blok-blok data. NameNode **tidak** menyimpan isi data aktual.
> - **DataNode (Worker):** Menyimpan potongan blok data fisik sebenarnya (default 128 MB per blok) dan mengirim laporan kesehatan (*heartbeat*) secara berkala ke NameNode.
> - **Secondary NameNode:** Melakukan penggabungan berkala (*checkpointing*) antara file `fsimage` dan `edit logs` agar proses boot NameNode tidak memakan waktu lama, bukan sekadar pengganti instan saat NameNode mati."

### Q3: Mengapa kita memilih Parquet daripada CSV untuk Big Data?
> **Jawaban:** "Ada 4 keunggulan utama Parquet dibanding CSV:
> 1. **Columnar Storage:** Data disimpan per kolom, bukan per baris. Kueri analitik hanya membaca kolom yang dibutuhkan (*Column Pruning*), sehingga I/O disk sangat hemat.
> 2. **Kompresi Tinggi:** Parquet secara native terkompresi (biasanya Snappy), menghemat ukuran file 70% sampai 80% dibanding CSV.
> 3. **Menyimpan Skema:** Tipe data (integer, string, timestamp) tersimpan permanen di metadata file, tidak perlu proses `inferSchema` lambat seperti CSV.
> 4. **Predicate Pushdown:** Filter data (`WHERE / filter`) dapat dievaluasi langsung di level penyimpanan sebelum dimuat ke memori."

### Q4: Apa itu Lazy Evaluation di Apache Spark?
> **Jawaban:** "**Lazy Evaluation** adalah strategi Spark di mana transformasi data (seperti `filter`, `select`, `join`) tidak langsung dieksekusi saat kode ditulis. Spark hanya mencatat rencana operasi dalam bentuk graf (*DAG / Directed Acyclic Graph*). Eksekusi fisik baru benar-benar berjalan ketika ada perintah bertipe **Action** (seperti `show()`, `count()`, atau `write`). Ini memungkinkan Spark mengoptimasi alur kerja secara global lewat *Catalyst Optimizer* sebelum menyentuh data."

### Q5: Kapan kita menggunakan Window Function dibanding GroupBy?
> **Jawaban:** "`groupBy` akan merangkum dan **memadatkan baris** menjadi satu baris per grup (agregasi murni). Sebaliknya, **Window Function** melakukan perhitungan agregasi atau perankingan per kelompok baris **tanpa menghilangkan detail baris aslinya**, sehingga sangat cocok untuk kasus mencari ranking (Top-N), running total, atau perbandingan antarbaris."

### Q6: Apa perbedaan antara `row_number()`, `rank()`, dan `dense_rank()`?
> **Jawaban:**
> - `row_number()`: Memberikan nomor urut unik berurutan (1, 2, 3, 4) tanpa peduli ada nilai yang kembar.
> - `rank()`: Nilai yang nilainya sama mendapat ranking yang sama, namun urutan berikutnya akan melompat (misal ada ranking kembar 2, maka urutannya: 1, 2, 2, 4).
> - `dense_rank()`: Nilai kembar mendapat ranking sama tanpa lompatan urutan (misal: 1, 2, 2, 3).

---

## 🛠️ Panduan Pertolongan Pertama (Troubleshooting Darurat)

| Masalah | Penyebab | Solusi Cepat |
|---|---|---|
| Perintah `hdfs` atau `start-dfs.sh` tidak ditemukan | Path belum terdaftar di session shell | Jalankan manual: `~/hadoop/sbin/start-dfs.sh` atau tambahkan path: `set -gx PATH ~/hadoop/bin ~/hadoop/sbin $PATH` |
| `NameNode` atau `DataNode` tidak muncul di `jps` | Service SSH belum jalan atau port macet | Jalankan `sudo systemctl start sshd`, lalu stop dan start ulang: `stop-dfs.sh && start-dfs.sh` |
| HDFS Safe Mode Error: `Cannot create directory... NameNode is in safe mode` | NameNode sedang melindungi diri saat startup | Paksa keluar dari safe mode: `hdfs dfsadmin -safemode leave` |
| PySpark Error: `Java gateway process exited before sending its port number` | Versi Java bentrok atau `JAVA_HOME` salah | Pastikan OpenJDK 11 terpasang dan tambahkan: `os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk"` sebelum buat SparkSession |
| PySpark Error: `Cannot assign requested address` | Masalah binding localhost di Linux | Tambahkan: `os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"` |
| File Parquet tidak bisa ditimpa | File sedang dibuka atau mode default error | Selalu gunakan `.mode("overwrite")` sebelum `.parquet(...)` |

---
*Semoga responsi praktikummu lancar dan mendapatkan nilai A! Buka file selanjutnya untuk melihat detail pembahasan masing-masing pertemuan.*
