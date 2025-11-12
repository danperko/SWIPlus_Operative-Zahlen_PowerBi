# Databricks notebook source
payload = {
    "resultType": "DATA_ONLY",
    "queryObject": {
        "columns": [
            {
                "name": "session_id",
                "scope": "OBJECT",
                "context": "SESSION",
                "lowerLimit": 1,
                "upperLimit": 5000000
            },
            {
                "name": "media",
                "scope": "OBJECT",
                "context": "ACTION"
            },
            {
                "name": "media_category_text_1",
                "scope": "OBJECT",
                "context": "ACTION"
            },
            {
                "name": "media_category_text_15",
                "scope": "OBJECT",
                "context": "ACTION"
            },
            {
                "name": "media_category_text_16",
                "scope": "OBJECT",
                "context": "ACTION"
            },
            {
                "name": "media_views",
                "columnPeriod": "ANALYSIS",
                "sortDirection": "DESCENDING",
                "sortIndex": 1,
                "scope": "OBJECT",
                "context": "ACTION"
            },
            {
                "name": "custom_metric_1000093",
                "columnPeriod": "ANALYSIS",
                "scope": "OBJECT",
                "context": "ACTION"
            },
            {
                "name": "custom_metric_1000088",
                "columnPeriod": "ANALYSIS",
                "scope": "OBJECT",
                "context": "ACTION"
            },
            {
                "name": "custom_metric_1000089",
                "columnPeriod": "ANALYSIS",
                "scope": "OBJECT",
                "context": "ACTION"
            },
            {
                "name": "custom_metric_1000090",
                "columnPeriod": "ANALYSIS",
                "scope": "OBJECT",
                "context": "ACTION"
            },
            {
                "name": "custom_metric_1000091",
                "columnPeriod": "ANALYSIS",
                "scope": "OBJECT",
                "context": "ACTION"
            },
            {
                "name": "media_viewsComplete",
                "columnPeriod": "ANALYSIS",
                "scope": "OBJECT",
                "context": "ACTION"
            },
            {
                "name": "media_playbackTime",
                "columnPeriod": "ANALYSIS",
                "scope": "OBJECT",
                "context": "ACTION"
            }
        ],
        "predefinedContainer": {
            "filters": [
                {
                    "name": "time_dynamic",
                    "connector": "AND",
                    "filterPredicate": "LIKE",
                    "value1": "yesterday",
                    "value2": "",
                    "context": "NONE",
                    "caseSensitive": False
                }
            ],
            "containers": [
                {
                    "filters": [
                        {
                            "name": "page_category_text_3",
                            "connector": "AND",
                            "filterPredicate": "LIKE",
                            "value1": "wasa app",
                            "value2": "",
                            "context": "PAGE",
                            "caseSensitive": False
                        }
                    ],
                    "containers": [],
                    "connector": "AND",
                    "context": "PAGE",
                    "inOrNotIn": "IN",
                    "type": "NORMAL"
                }
            ]
        },
        "variant": "PIVOT_AS_LIST"
    }
}

# COMMAND ----------

import copy

def replace_time_filter(payload, start_date, end_date):
    """Ersetzt den Filter 'time_dynamic' durch 'time_range'."""
    new_payload = copy.deepcopy(payload)
    new_payload["queryObject"]["predefinedContainer"]["filters"] = [
        {
            "name": "time_range",
            "value1": start_date,
            "value2": end_date,
            "filterPredicate": "BETWEEN",
            "connector": "AND",
            "context": "NONE",
            "caseSensitive": False
        }
    ]
    return new_payload
# Wenn Daten *manuell* nachgeladen werden müssen, führen Sie die folgende Zeile aus.
# Andernfalls bitte auskommentiert lassen und nicht ausführen.
#payload = replace_time_filter(payload, "2025-10-29 00:00:00", "2025-11-05 00:00:00")

# COMMAND ----------

import requests
import os
import json
import time  # Add this import

# Zugangsdaten (SECRET_SCOPE = "swi-secret-scope"
# Für Hilfe: Keller, Pascal (SRF) oder hier: https://github.com/mmz-srf/swi-analytics-databricks/blob/main/intelligence.eu.mapp.com_analysis-query_7455/Secret%20management%20in%20databricks.ipynb)
SECRET_SCOPE = "swi-secret-scope"
user   = dbutils.secrets.get(SECRET_SCOPE, "mapp-user")
secret = dbutils.secrets.get(SECRET_SCOPE, "mapp-secret")
try:
    baseurl = dbutils.secrets.get(SECRET_SCOPE, "mapp-baseurl")
except:
    baseurl = "https://intelligence.eu.mapp.com"

token_file = 'mapp_token.json'

# Prüfen, ob bereits ein Token existiert und ob es noch gültig ist
def get_token():
    # Prüfen, ob bereits ein Token existiert und ob es noch gültig ist
    if os.path.exists(token_file):
        with open(token_file, 'r') as f:
            data = json.load(f)
            token = data.get('access_token')
            expires_at = data.get('expires_at')
            if token and expires_at and time.time() < expires_at:
                return token  # ⏳ Noch gültig

    # 🆕 Token holen
    auth_url = f"{baseurl}/analytics/api/oauth/token"
    querystring = {"grant_type": "client_credentials", "scope": "mapp.intelligence-api"}
    response = requests.post(auth_url, auth=(user, secret), params=querystring)
    response.raise_for_status()
    result = response.json()
    token = result['access_token']
    expires_in = result.get('expires_in', 3600)  # meist 3600 Sekunden
    expires_at = time.time() + expires_in - 60   # etwas Puffer

    # Token speichern für später
    with open(token_file, 'w') as f:
        json.dump({'access_token': token, 'expires_at': expires_at}, f)

    return token

# Token abrufen
token = get_token()

# COMMAND ----------

import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

# SparkSession holen
spark = SparkSession.builder.getOrCreate()

# Auth-Header
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# API Query
url = f"{baseurl}/analytics/api/analysis-query"
response = requests.post(url, headers=headers, json=payload)
result = response.json()

resultUrl = result.get("resultUrl")
statusUrl = result.get("statusUrl")

# ggf. warten
tries = 0
while not resultUrl and tries < 10:
    time.sleep(10)
    status_response = requests.get(statusUrl, headers=headers)
    result = status_response.json()
    resultUrl = result.get("resultUrl")
    tries += 1

if not resultUrl:
    print("❌ Kein Ergebnis nach mehreren Versuchen.")
else:
    result_data = requests.get(resultUrl, headers=headers).json()

    # Spaltennamen und Zeilen extrahieren
    headers_out = [col["name"] for col in result_data["headers"]]
    rows = result_data["rows"]

    # DataFrame erstellen
    df = spark.createDataFrame(rows, headers_out)

   
    
    # 🔻 Letzte Zeile entfernen
    row_count = df.count()
    if row_count > 1:
        df = df.limit(row_count - 1)
    else:
        print("⚠️ Zu wenige Zeilen zum Kürzen.")

    # Ergebnisse anzeigen (optional)
    display(df)

    # ✅ Speichern als Delta Table (Version 2.0)
    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("swi_audience_prd.swiplus.mapp_api_query_7241_08")