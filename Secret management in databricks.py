# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Notebook: Secrets verwalten & mit Webtrekk nutzen
# MAGIC 📦 Voraussetzungen
# MAGIC
# MAGIC Secret-Scope existiert: swi-secrets
# MAGIC
# MAGIC - In swi-secrets existiert ein Secret mit einem Databricks PAT (z. B. Key dbx-pat) (Settings → Developer → Personal Access Tokens → Token erzeugen → in swi-secrets ablegen)
# MAGIC - Workspace-URL (ohne Trailing Slash): https://adb-4119964566130471.11.azuredatabricks.net

# COMMAND ----------

# MAGIC %md
# MAGIC # 👀 Schritt 1: Scopes & Secret-Keys anzeigen

# COMMAND ----------

# Zeige alle Secret Scopes mit dbutils
scopes = dbutils.secrets.listScopes()
display([{"name": s.name} for s in scopes])

# COMMAND ----------

# MAGIC %md
# MAGIC # ➕ Schritt 2: Neuse Secrets anlegen / sectets überschreiben

# COMMAND ----------

from databricks.sdk import WorkspaceClient
 
w = WorkspaceClient()
SECRET_SCOPE = "swi-secret-scope" # Ein bestehender Scope aus der Lise dbutils.secrets.listScopes(). für andere Scopes siehe oben
KEY_NAME = "<key-name>" # Ein beliebiger name
SECRET = "<secret>" # Ein beliebiger Wert
w.secrets.put_secret(SECRET_SCOPE,KEY_NAME,string_value =SECRET) # generische art ein secret zu erstellen. 

#KOnkrete secrets (zum ändern string_value mit echtem wert anpassen anpassen und code laoifen lassen)
#w.secrets.put_secret("swi-secret-scope","mapp-user",string_value ="<secret>")
#w.secrets.put_secret("swi-secret-scope","mapp-secret",string_value ="<secret>")
#w.secrets.put_secret("swi-secret-scope","mapp-baseurl",string_value ="<secret>")


# COMMAND ----------

dbutils.secrets.get(SECRET_SCOPE, KEY_NAME) # Asugabe des eben erstellten wertes

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔑 Exkurs: Webtrekk/Mapp Token generieen mit Umgebungsvariablen (Secrets)

# COMMAND ----------

import os, time, json, requests

TOKEN_PATH = "./mapp_token.json"

def _read_cached_token(path=TOKEN_PATH):
    if os.path.exists(path):
        with open(path, "r") as f:
            data = json.load(f)
        if time.time() < data.get("expires_at", 0):
            return data["access_token"]
    return None

def _write_cached_token(token, expires_in, path=TOKEN_PATH, safety_margin=60):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    expires_at = time.time() + int(expires_in) - safety_margin
    with open(path, "w") as f:
        json.dump({"access_token": token, "expires_at": expires_at}, f)

def get_mapp_token(user=MAPP_USER, secret=MAPP_SECRET, base=MAPP_BASE):
    cached = _read_cached_token()
    if cached:
        return cached
    url = f"{base}/analytics/api/oauth/token"
    qs  = {"grant_type": "client_credentials", "scope": "mapp.intelligence-api"}
    r = requests.post(url, auth=(user, secret), params=qs, timeout=30)
    r.raise_for_status()
    res = r.json()
    _write_cached_token(res["access_token"], res.get("expires_in", 3600))
    return res["access_token"]

# Test
tok = get_mapp_token()
("OK", tok[:8] + "…")


# COMMAND ----------

df = mapp_analysis_query(payload)
display(df)
