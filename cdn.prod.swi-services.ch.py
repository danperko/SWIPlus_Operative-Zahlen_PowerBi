# Databricks notebook source
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

# 🔐 Zugangsdaten. Ask David.Schwelien for credentials. HINT: Can be found in SWI Password vauld "1Password"]
username = dbutils.secrets.get(SECRET_SCOPE, "cdn.prod.swi-services.ch_username") 
password = dbutils.secrets.get(SECRET_SCOPE, "cdn.prod.swi-services.ch_password")
auth = HTTPBasicAuth(username, password)

# 📅 Zeitraum definieren
start_year = 2025
end_year = 2030
months = range(1, 13)  # Januar bis Dezember

# 🌍 Basis-URL
base_url = "https://cdn.prod.swi-services.ch/report/content/"

# 📥 Alle Daten sammeln
all_data = []

for year in range(start_year, end_year + 1):
    for month in months:
        url = f"{base_url}{year}/{month}"
        print(f"🔄 Lade {url} ...")
        
        response = requests.get(url, auth=auth)
        
        if response.status_code == 200:
            try:
                json_data = response.json()
                if isinstance(json_data, list) and json_data:
                    df = pd.json_normalize(json_data)
                    df["year"] = year
                    df["month"] = month
                    all_data.append(df)
            except Exception as e:
                print(f"⚠️ Fehler beim Parsen von {url}: {e}")
        else:
            print(f"❌ Fehler {response.status_code} bei {url}")

# 🔗 Zusammenführen
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    
    # 🧹 Datumsfelder optional konvertieren
    if "published" in final_df.columns:
        final_df["published"] = pd.to_datetime(final_df["published"], errors='coerce')
    
    print("✅ Gesamtanzahl geladener Zeilen:", len(final_df))
    display(final_df.head())
else:
    print("⚠️ Keine Daten geladen.")

  # Schritt 1: Daten anhängen
(
    spark.createDataFrame(final_df).write
    .option("mergeSchema", "true")
    .mode("append")
    .format("delta")
    .saveAsTable("swi_audience_prd.swiplus.cdn_prod_swi_services_ch")
)

# Schritt 2: Duplikate entfernen und neu schreiben
df = spark.read.table("swi_audience_prd.swiplus.cdn_prod_swi_services_ch")
df_deduped = df.dropDuplicates(["wpPostId"])

(
    df_deduped.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("swi_audience_prd.swiplus.cdn_prod_swi_services_ch")
)
