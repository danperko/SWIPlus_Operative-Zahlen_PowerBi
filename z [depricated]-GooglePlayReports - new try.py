# Databricks notebook source
import gcsfs
import shutil
import os

# Pfad zum Service Account Key
key_path = "/Volumes/swi_audience_prd/swiplus/blob/swissinfo-987ec-aba5c89e8808.json"

# GCS Zugriff initialisieren
fs = gcsfs.GCSFileSystem(token=key_path)

# Quelle und Ziel
gcs_folder = 'pubsite_prod_rev_11741298576716878675/stats/installs/'
target_dir = "/Volumes/swi_audience_prd/swiplus/play_reports/"

# Temporäres lokales Verzeichnis
tmp_dir = "/tmp/gcs_download"
os.makedirs(tmp_dir, exist_ok=True)

# Volume-Zielverzeichnis ggf. leeren
for f in os.listdir(target_dir):
    os.remove(os.path.join(target_dir, f))

# Liste aller CSV-Dateien
files = fs.ls(gcs_folder)

# Alle Dateien zuerst nach /tmp speichern
for file_path in files:
    filename = file_path.split("/")[-1]
    tmp_path = os.path.join(tmp_dir, filename)
    target_path = os.path.join(target_dir, filename)

    with fs.open(file_path, 'rb') as remote_f, open(tmp_path, 'wb') as tmp_f:
        shutil.copyfileobj(remote_f, tmp_f, length=64 * 1024)  # 64KB-Chunks

    # Nach dem Zwischenspeichern ins Volume kopieren
    shutil.copyfile(tmp_path, target_path)
    print(f"✅ Gespeichert: {filename}")


# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import os
import re
from collections import defaultdict

# 🔁 Pfad zu deinem Volume mit CSVs
volume_path = target_dir

# Alle CSV-Dateien im Volume finden
all_files = [f for f in os.listdir(volume_path) if f.endswith(".csv")]

# Typen extrahieren aus Dateinamen
pattern = r"installs_.*_(\d{6})_(\w+)\.csv"
file_groups = defaultdict(list)

for file in all_files:
    match = re.match(pattern, file)
    if match:
        file_type = match.group(2)
        file_groups[file_type].append(os.path.join(volume_path, file))

# Spark starten
spark = SparkSession.builder.getOrCreate()

# Funktion zum Kombinieren mit `source_file` (_metadata.file_path für Unity Catalog)
def load_and_union_csvs_with_file_path(file_list):
    df_all = None
    for f in file_list:
        df = (
            spark.read.option("header", True)
            .option("encoding", "UTF-16")  # oder alternativ "windows-1252"
            .option("recursiveFileLookup", "true")
            .option("includeMetadata", "true")
            .csv(f)
            .withColumn("source_file", col("_metadata.file_path"))
        )
        
        # Spaltennamen bereinigen
        for colname in df.columns:
            new_colname = re.sub(r"[ ,;{}()\n\t=]", "_", colname)
            df = df.withColumnRenamed(colname, new_colname)

        df_all = df if df_all is None else df_all.unionByName(df, allowMissingColumns=True)
    return df_all


# ✅ Alle Typen verarbeiten und als Tabelle schreiben
for file_type, file_list in file_groups.items():
    print(f"📥 Verarbeite Typ: {file_type} ({len(file_list)} Dateien)")

    df_combined = load_and_union_csvs_with_file_path(file_list)
    
    output_table = f"swi_audience_prd.swiplus.playstore_installs_{file_type}"
    
    (
        df_combined.write
        .format("delta")
        .mode("overwrite")  # Oder "append", wenn du nur neue Dateien anhängst
        .option("overwriteSchema", "true")
        .saveAsTable(output_table)
    )
    
    print(f"✅ Gespeichert als: {output_table}")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, expr, when, lit
from pyspark.sql.types import IntegerType, DoubleType

# Tabellenliste
# Tabellenliste
table_names = [
    "swi_audience_prd.swiplus.playstore_installs_carrier",
    "swi_audience_prd.swiplus.playstore_installs_app_version",
    "swi_audience_prd.swiplus.playstore_installs_country",
    "swi_audience_prd.swiplus.playstore_installs_device",
    "swi_audience_prd.swiplus.playstore_installs_language",
    "swi_audience_prd.swiplus.playstore_installs_os_version",
    "swi_audience_prd.swiplus.playstore_installs_overview",
    "swi_audience_prd.swiplus.playstore_installs_tablets",
]

for table in table_names:
    print(f"🔄 Verarbeite Tabelle: {table}")
    
    # Tabelle laden
    df = spark.table(table)
    
    # 1. Sonderzeichen � entfernen aus allen Spalten
    for c in df.columns:
        df = df.withColumn(c, regexp_replace(col(c).cast("string"), "�", ""))
    
    # 2. "Date"-Spalte in DateType umwandeln (mit Prüfung)
    if "Date" in df.columns:
        valid_date_expr = col("Date").rlike(r"^\d{4}-\d{2}-\d{2}$")
        df = df.withColumn(
            "Date",
            when(valid_date_expr, to_date("Date", "yyyy-MM-dd")).otherwise(lit(None))
        )

    # 3. Leere Spalten entfernen
    non_null_columns = [c for c in df.columns if df.select(c).na.drop().count() > 0]
    df = df.select(*non_null_columns)
    
    # 4. Typen analysieren & ggf. konvertieren (ohne RDDs, mit try_cast)
    for c in df.columns:
        if c == "Date":
            continue
        try:
            sample_values = (
                df.select(c).dropna().limit(1000).toPandas()[c].astype(str).tolist()
            )
            # Versuche Integer
            if all(v.strip().isdigit() for v in sample_values if v.strip() != ""):
                df = df.withColumn(c, expr(f"try_cast({c} as INT)"))
            # Versuche Float (Punkt als Dezimaltrenner)
            elif all(v.replace(".", "", 1).isdigit() for v in sample_values if v.strip() != ""):
                df = df.withColumn(c, expr(f"try_cast({c} as DOUBLE)"))
        except:
            pass  # Bei Fehler keine Typumwandlung

    # 5. Tabelle speichern
    target_table = table + "_cleaned"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"✅ Bereinigt und gespeichert als: {target_table}")
