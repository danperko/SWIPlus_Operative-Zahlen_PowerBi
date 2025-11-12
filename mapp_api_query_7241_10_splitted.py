# Databricks notebook source
# =========================
# Split & Normalization for pages-like columns
# Keep ALL original columns and save result as a new Delta table
# =========================

from pyspark.sql import functions as F, types as T

# --- Quelle laden ---
table = "swi_audience_prd.swiplus.mapp_api_query_7241_10"
df = spark.table(table)

# WICHTIG: Originalspalten sichern, bevor neue Spalten hinzugefügt werden
original_cols = df.columns

# --- Zu splittende Spalten ---
source_cols = [
    "pages",
    "pages_preceeding1",
    "pages_preceeding2",
    "pages_follower1",
    "pages_follower2",
]

# --- Wissensbasis / Anker ---
LANGS = {"de","en","fr","it","es","pt","ar","ru","ja","zh","tr","fa","hi","uk","pl"}
EXPECTED_BRAND = "swi"
EXPECTED_PLATFORM = "app"
KNOWN_APPS = {"wasa_app"}               # bei Bedarf ergänzen
KNOWN_SITES = {"swissinfo","virtualsite"}

def is_screen_token(tok: str) -> bool:
    # alles was wie ein Screen aussieht
    return tok == "page" or tok.endswith("-screen") or tok in {"briefing"}

# --- UDF-Schema ---
schema = T.StructType([
    T.StructField("seg01", T.StringType()),  # brand
    T.StructField("seg02", T.StringType()),  # platform
    T.StructField("seg03", T.StringType()),  # app_name
    T.StructField("seg04", T.StringType()),  # site
    T.StructField("seg05", T.StringType()),  # screen
    T.StructField("seg06", T.StringType()),  # tail
    T.StructField("note",  T.StringType()),
])

# --- Normalisierung / inhaltliche Zuordnung ---
def normalize_entry(s: str):
    import re
    note = []

    if s is None:
        return (None, None, None, None, None, None, "empty")

    # Pre-Clean
    x = s.strip().lower().replace(" ", "_")
    x = re.sub(r"\.{2,}", ".", x)              # .. -> .
    x = re.sub(r"^\.+|\.+$", "", x)            # führende/trailing Punkte weg
    if not x:
        return (None, None, None, None, None, None, "empty_after_cleanup")

    parts = [p for p in x.split(".") if p != ""]
    if not parts:
        return (None, None, None, None, None, None, "no_parts")

    # Helper: erstes Element, das Prädikat erfüllt, zurückgeben & entfernen
    def pop_first(ps, pred):
        for i, t in enumerate(ps):
            if pred(t):
                return ps.pop(i)
        return None

    # Sprachcodes vor "app" entfernen (Fall: SWI.de.app.…)
    if "app" in parts:
        app_idx = parts.index("app")
        kept, removed_langs = [], []
        for i, tok in enumerate(parts):
            if i < app_idx and tok in LANGS:
                removed_langs.append(tok)
            else:
                kept.append(tok)
        if removed_langs:
            note.append(f"removed_langs={','.join(removed_langs)}")
        parts = kept

    seg01 = seg02 = seg03 = seg04 = seg05 = seg06 = None

    # Brand
    if EXPECTED_BRAND in parts:
        pop_first(parts, lambda t: t == EXPECTED_BRAND)
        seg01 = EXPECTED_BRAND
    else:
        # fallback: erstes Token als Brand interpretieren
        seg01 = parts.pop(0)

    # Platform
    if "app" in parts:
        pop_first(parts, lambda t: t == "app")
        seg02 = "app"
    else:
        note.append("no_app")

    # App-Name (bekannte Apps bevorzugt, sonst *_app)
    seg03 = pop_first(parts, lambda t: t in KNOWN_APPS) or pop_first(parts, lambda t: t.endswith("_app"))

    # Site (bevorzugt bekannte Sites)
    site = pop_first(parts, lambda t: t in KNOWN_SITES)
    if site:
        seg04 = site

    # Screen (…-screen, page, briefing,…)
    screen = pop_first(parts, is_screen_token)
    if screen:
        seg05 = screen

    # Tail (alles Übrige zusammenführen: z. B. '/today', 'home', 'today.today')
    tail = ".".join(parts) if parts else None
    seg06 = tail if tail else None

    # Hinweise
    if seg01 != EXPECTED_BRAND:
        note.append(f"brand={seg01}")
    if seg02 and seg02 != EXPECTED_PLATFORM:
        note.append(f"platform={seg02}")
    if seg04 and seg04 not in KNOWN_SITES:
        note.append(f"site_unexpected={seg04}")
    if not seg04:
        note.append("no_site")
    if not seg05:
        note.append("no_screen")

    note_str = ",".join(note) if note else None
    return (seg01, seg02, seg03, seg04, seg05, seg06, note_str)

normalize_udf = F.udf(normalize_entry, schema)

# --- Anwendung auf alle relevanten Spalten + Originalwert *_full ---
for c in source_cols:
    out = normalize_udf(F.col(c))
    df = (
        df
        .withColumn(f"{c}_01",  out["seg01"])
        .withColumn(f"{c}_02",  out["seg02"])
        .withColumn(f"{c}_03",  out["seg03"])
        .withColumn(f"{c}_04",  out["seg04"])
        .withColumn(f"{c}_05",  out["seg05"])
        .withColumn(f"{c}_06",  out["seg06"])
        .withColumn(f"{c}_note",out["note"])
        .withColumn(f"{c}_full",F.col(c))    # Originalwert beilegen
    )

# --- Ergebnis zusammenstellen: ALLE Originalspalten + neue Spalten ---
full_cols  = [f"{c}_full" for c in source_cols]
note_cols  = [f"{c}_note" for c in source_cols]
split_cols = [f"{c}_{i:02d}" for c in source_cols for i in range(1, 7)]

result_df = df.select(*original_cols, *full_cols, *split_cols, *note_cols)

# --- Vorschau anzeigen ---
display(result_df)

# --- Speichern als Delta-Tabelle ---
(
    result_df.write
    .format("delta")
    .mode("overwrite")                 # komplette Neuschreibung
    .option("overwriteSchema", "true") # Schema-Änderungen zulassen
    .saveAsTable("swi_audience_prd.swiplus.mapp_api_query_7241_10_splitted")
)

# Optional: sinnvolle Table Properties setzen
spark.sql("""
  ALTER TABLE swi_audience_prd.swiplus.mapp_api_query_7241_10_splitted
  SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite=true, delta.autoOptimize.autoCompact=true)
""")

print("Written table: swi_audience_prd.swiplus.mapp_api_query_7241_10_splitted")
