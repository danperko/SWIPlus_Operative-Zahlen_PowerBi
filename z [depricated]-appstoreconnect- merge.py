# Databricks notebook source
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col


# 🔁 Pfad zu deinem Ordner mit den CSV-Dateien
folder_path = r"/Volumes/swi_audience_prd/swiplus/appstore_sales_daily"

# 🔍 Alle CSV-Dateien im Ordner finden
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# 📦 Alle Dateien laden und mit Dateinamen versehen
dataframes = []
for file in csv_files:
    file_path = os.path.join(folder_path, file)
    df = pd.read_csv(file_path, sep='\t', dtype=str)  # dtype=str verhindert falsche automatische Konvertierung
    df["Source.Name"] = file  # ⬅ Dateiname als Spalte hinzufügen
    dataframes.append(df)

# 🔗 Alle DataFrames zusammenfügen
merged_df_pandas = pd.concat(dataframes, ignore_index=True)

# 🧹 Spaltennamen bereinigen
def clean_column_name(name):
    """Ersetzt ungültige Zeichen in einem Spaltennamen durch Unterstriche."""
    return ''.join('_' if c in ' ,;{}()\n\t=' else c for c in name)

merged_df_pandas.columns = [clean_column_name(col) for col in merged_df_pandas.columns]

# Sicherstellen, dass alle Spalten Strings sind
merged_df_pandas = merged_df_pandas.astype(str)

# Umwandlung in Spark DataFrame
merged_df_spark = spark.createDataFrame(merged_df_pandas)

# Tabelle speichern
merged_df_spark.write.mode("overwrite").saveAsTable("swi_audience_prd.swiplus.appstore_sales_merged")
# Sicherstellen, dass alle Spalten Strings sind
merged_df_pandas = merged_df_pandas.astype(str)

# Umwandlung in Spark DataFrame
merged_df_spark = spark.createDataFrame(merged_df_pandas)

# timestamp-Spalten ergänzen
merged_df_spark = merged_df_spark.withColumn("Begin_Date_Timestamp", to_timestamp(col("Begin_Date"), "MM/dd/yyyy"))
merged_df_spark = merged_df_spark.withColumn("End_Date_Timestamp", to_timestamp(col("End_Date"), "MM/dd/yyyy"))

# Tabelle speichern
merged_df_spark.write \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("swi_audience_prd.swiplus.appstore_sales_merged")