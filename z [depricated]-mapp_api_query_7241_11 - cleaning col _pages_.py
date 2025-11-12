# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Spark Session (nur nötig, falls du lokal testest)
spark = SparkSession.builder.getOrCreate()

# 1. Tabelle laden
df = spark.table("swi_audience_prd.swiplus.mapp_api_query_7241_11")

# 2. Hilfsfunktion definieren
def clean_page(value):
    if value is None:
        return None
    # Schrittweise Bereinigung
    value = value.replace("swi.app.wasa_app.swissinfo.", "")
    value = value.replace("f1", "")
    
    # Sprachcodes entfernen
    for lang in ["eng", "ger", "fre", "ita"]:
        value = value.replace("." + lang + ".", ".")

    # Sonderfall: alles nach 'post.' behalten (inkl. ID)
    if "post." in value:
        parts = value.split("post.")
        value = parts[0] + "post." + parts[1].strip("-.")
    else:
        # Entferne UUIDs / lange IDs
        value = ".".join([part for part in value.split(".") if not part.replace("-", "").isdigit()])

    # Entferne alle '-' und abschließende '.'
    value = value.replace("-", "")
    value = value.rstrip(".")

    return value

# 3. UDF registrieren
clean_page_udf = F.udf(clean_page, returnType=F.StringType())

# 4. Neue Spalte berechnen
df_cleaned = df.withColumn("pages_cleaned", clean_page_udf(F.col("pages")))

df_cleaned

# 5. Überschreibe Tabelle
(
    df_cleaned.write
    .option("mergeSchema", "true")  # ✅ Schema-Anpassung aktivieren
    .mode("overwrite")
    .format("delta")
    .saveAsTable("swi_audience_prd.swiplus.mapp_api_query_7241_11")
)

# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

df_cleaned.groupBy("pages_cleaned").count().orderBy(F.desc("count")).show(truncate=False)
