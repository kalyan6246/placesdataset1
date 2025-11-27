from flask import Flask, jsonify, render_template
import json
import os
from google.cloud import bigquery
from datetime import date

app = Flask(__name__)

# -------------------------------
# CONFIG
# -------------------------------
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "NVMB.geojson")

BQ_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET", "places_dataset")
BQ_TABLE = os.environ.get("BQ_TABLE", "maharashtra_pois")

# -------------------------------
# LOAD GEOJSON
# -------------------------------
with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
    geojson_data = json.load(f)

# Initialize BigQuery client
bq_client = bigquery.Client()

# -------------------------------
# CREATE DATASET + TABLE IF NOT EXISTS
# -------------------------------
def ensure_bq_resources():
    # Dataset
    dataset_ref = bigquery.Dataset(f"{BQ_PROJECT}.{BQ_DATASET}")
    try:
        bq_client.get_dataset(dataset_ref)
    except:
        bq_client.create_dataset(dataset_ref)
        print(f"Created dataset {BQ_DATASET}")

    # Table schema
    schema = [
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("ingestion_date", "DATE"),
        bigquery.SchemaField("geometry", "GEOGRAPHY"),
    ]

    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    try:
        bq_client.get_table(table_ref)
    except:
        table_obj = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table_obj)
        print(f"Created table {BQ_TABLE}")

# -------------------------------
# LOAD POLYGON GEOJSON INTO BIGQUERY
# -------------------------------
def load_geojson_into_bq():
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    rows = []

    for feat in geojson_data.get("features", []):
        props = feat.get("properties", {})
        geom = feat.get("geometry")

        if geom is None:
            continue

        # Convert entire geometry to GEOGRAPHY string
        geography_value = json.dumps(geom)

        rows.append({
            "name": props.get("name", "unknown"),
            "type": props.get("type", "polygon"),
            "ingestion_date": date.today().isoformat(),
            "geometry": geography_value,
        })

    if not rows:
        print("No data to insert into BigQuery")
        return

    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        print("BigQuery insert errors:", errors)
    else:
        print(f"Inserted {len(rows)} polygon records into BigQuery.")

# -------------------------------
# RUN AT STARTUP
# -------------------------------
print("Ensuring dataset/table...")
ensure_bq_resources()

print("Loading polygon GeoJSON into BigQuery...")
load_geojson_into_bq()

# -------------------------------
# FLASK ROUTES
# -------------------------------
@app.route("/geojson")
def geojson():
    return jsonify(geojson_data)

@app.route("/")
def map_view():
    return render_template("map.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
