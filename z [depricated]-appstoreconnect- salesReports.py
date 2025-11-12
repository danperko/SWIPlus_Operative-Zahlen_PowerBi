# Databricks notebook source
import jwt
import time
import requests
import gzip
from datetime import datetime, timedelta


# Zugangsdaten
ISSUER_ID = "69a6de84-c71e-47e3-e053-5b8c7c11a4d1"
KEY_ID = "NCN84Y5H2A"
VENDOR_NUMBER = "85120192"
PRIVATE_KEY_PATH = "/Volumes/swi_audience_prd/swiplus/blob/AuthKey_NCN84Y5H2A.p8"
REPORT_DATE = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d') # Dynamisches Datum: vorletzter Tag
REPORT_NAME = f"appstore_sales_daily_{REPORT_DATE}.csv.gz"
TEMP_LOCAL_PATH = f"/tmp/{REPORT_NAME}"
TARGET_PATH = f"/Volumes/swi_audience_prd/swiplus/appstore_sales_daily/{REPORT_NAME}"

# 🔐 Private Key aus Datei lesen
with open(PRIVATE_KEY_PATH, 'r') as f:
    PRIVATE_KEY = f.read()

# 🔑 JWT Token erstellen
token = jwt.encode(
    {
        'iss': ISSUER_ID,
        'exp': int(time.time()) + 600,
        'aud': 'appstoreconnect-v1'
    },
    PRIVATE_KEY,
    algorithm='ES256',
    headers={'alg': 'ES256', 'kid': KEY_ID}
)

# 📥 Anfrage-URL vorbereiten
url = (
    "https://api.appstoreconnect.apple.com/v1/salesReports"
    f"?filter[reportType]=SALES"
    f"&filter[reportSubType]=SUMMARY"
    f"&filter[vendorNumber]={VENDOR_NUMBER}"
    f"&filter[frequency]=DAILY"
    f"&filter[reportDate]={REPORT_DATE}"
    f"&filter[version]=1_1"  # Aktuelle Version für DAILY-Berichte
)

headers = {
    'Authorization': f'Bearer {token}',
    'Accept': 'application/a-gzip'
}

# ⬇️ Request absenden und lokal zwischenspeichern
response = requests.get(url, headers=headers)

if response.status_code == 200:
    with open(TEMP_LOCAL_PATH, "wb") as tmp_file:
        tmp_file.write(response.content)
    
    # Datei entpacken
    with gzip.open(TEMP_LOCAL_PATH, 'rt', encoding='utf-8') as f_in:
        file_content = f_in.read()

    # 📤 Datei in Blob-Verzeichnis schreiben
    dbutils.fs.put(f"dbfs:{TARGET_PATH.replace('.gz', '')}", file_content, overwrite=True)
    print(f"✅ Erfolgreich gespeichert: {TARGET_PATH.replace('.gz', '')}")
else:
    print(f"❌ Fehler: {response.status_code}")
    print(response.text)
