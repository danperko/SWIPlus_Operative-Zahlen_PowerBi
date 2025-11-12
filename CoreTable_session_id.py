# Databricks notebook source
# Katalog und Schema setzen
spark.sql("USE CATALOG swi_audience_prd")
SCHEMA = "swiplus"

# Problematische Tabellen ausschliessen
BLACKLIST = {
    "mapp_api_query_7241_06_enriched",
    "mapp_api_query_7241_08_clone",
    "vw_mapp_api_query_7241_with_formatted_time",
}

# Alle Tabellen im Schema holen
tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN `swi_audience_prd`.`{SCHEMA}`").collect()]

def fqtn(t): 
    return f"`swi_audience_prd`.`{SCHEMA}`.`{t}`"

def has_column(fq_table, col_name):
    cols = [r["col_name"].lower() for r in spark.sql(f"DESCRIBE {fq_table}").collect() if r["col_name"]]
    return col_name.lower() in cols

session_ids = None

for t in tables:
    if t in BLACKLIST:
        continue
    full = fqtn(t)
    if not has_column(full, "session_id"):
        continue
    df = spark.sql(f"SELECT session_id FROM {full} WHERE session_id IS NOT NULL")
    if df.limit(1).count() == 0:
        continue
    session_ids = df if session_ids is None else session_ids.unionByName(df)

# Falls Daten gefunden → distinct & speichern
if session_ids is not None:
    session_ids = session_ids.distinct()
    session_ids.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("swi_audience_prd.swiplus.CoreTable_session_id")
    print("✅ Session-IDs gespeichert unter swi_audience_prd.swiplus.CoreTable_session_id")
else:
    print("⚠️ Keine Session-IDs gefunden.")
