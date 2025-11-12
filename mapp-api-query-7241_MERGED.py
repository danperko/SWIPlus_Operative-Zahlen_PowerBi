# Databricks notebook source
from pyspark.sql.functions import col, when, length, concat_ws, lit, substring, to_timestamp, to_date



# Tabelle einlesen (unverändert)
df1 = spark.table("swi_audience_prd.swiplus.mapp_api_query_7241_01")
df2 = spark.table("swi_audience_prd.swiplus.mapp_api_query_7241_02")
df3 = spark.table("swi_audience_prd.swiplus.mapp_api_query_7241_03")
df4 = spark.table("swi_audience_prd.swiplus.mapp_api_query_7241_04")
df5 = spark.table("swi_audience_prd.swiplus.mapp_api_query_7241_05")

# Spalten umbenennen
def rename_columns(df, suffix):
    return df.select([col(c).alias(f"{c}{suffix}") if c != "session_id" else col(c) for c in df.columns])

df1 = rename_columns(df1, "_01")
df2 = rename_columns(df2, "_02")
df3 = rename_columns(df3, "_03")
df4 = rename_columns(df4, "_04")
df5 = rename_columns(df5, "_05")

# Join
merged_df = df1 \
    .join(df2, on="session_id", how="outer") \
    .join(df3, on="session_id", how="outer") \
    .join(df4, on="session_id", how="outer") \
    .join(df5, on="session_id", how="outer")


# Neue Timestamp-Spalte direkt in Spark erzeugen
merged_df = merged_df.withColumn(
    "visit_time_ts",
    when(
        length(col("visit_time_02")) == 14,
        to_timestamp(
            concat_ws("",
                substring("visit_time_02", 1, 4), lit("-"),
                substring("visit_time_02", 5, 2), lit("-"),
                substring("visit_time_02", 7, 2), lit(" "),
                substring("visit_time_02", 9, 2), lit(":"),
                substring("visit_time_02", 11, 2), lit(":"),
                substring("visit_time_02", 13, 2)
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    ).otherwise(None)
)


merged_df = merged_df.withColumn("visit_date", to_date("visit_time_ts"))

# Ergebnis anzeigen
display(merged_df)

# Speichern mit Schema Evolution
merged_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("swi_audience_prd.swiplus.mapp_api_query_7241_MERGED")


# COMMAND ----------

from pyspark.sql.functions import col, when, length, concat_ws, lit, substring, to_timestamp, to_date



# Tabelle einlesen (unverändert)
df1 = spark.table("swi_audience_prd.swiplus.mapp_api_query_7241_01")
df2 = spark.table("swi_audience_prd.swiplus.mapp_api_query_7241_02")
df3 = spark.table("swi_audience_prd.swiplus.mapp_api_query_7241_03")
df4 = spark.table("swi_audience_prd.swiplus.mapp_api_query_7241_04")
df5 = spark.table("swi_audience_prd.swiplus.mapp_api_query_7241_05")

# Spalten umbenennen
def rename_columns(df, suffix):
    return df.select([col(c).alias(f"{c}{suffix}") if c != "session_id" else col(c) for c in df.columns])

df1 = rename_columns(df1, "_01")
df2 = rename_columns(df2, "_02")
df3 = rename_columns(df3, "_03")
df4 = rename_columns(df4, "_04")
df5 = rename_columns(df5, "_05")

# Join
merged_df = df1 \
    .join(df2, on="session_id", how="outer") \
    .join(df3, on="session_id", how="outer") \
    .join(df4, on="session_id", how="outer") \
    .join(df5, on="session_id", how="outer")


# Neue Timestamp-Spalte direkt in Spark erzeugen
merged_df = merged_df.withColumn(
    "visit_time_ts",
    when(
        length(col("visit_time_02")) == 14,
        to_timestamp(
            concat_ws("",
                substring("visit_time_02", 1, 4), lit("-"),
                substring("visit_time_02", 5, 2), lit("-"),
                substring("visit_time_02", 7, 2), lit(" "),
                substring("visit_time_02", 9, 2), lit(":"),
                substring("visit_time_02", 11, 2), lit(":"),
                substring("visit_time_02", 13, 2)
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    ).otherwise(None)
)


merged_df = merged_df.withColumn("visit_date", to_date("visit_time_ts"))

# Ergebnis anzeigen
display(merged_df)

# Speichern mit Schema Evolution
merged_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("swi_audience_prd.swiplus.mapp_api_query_7241_MERGED")
