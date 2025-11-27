from flask import Flask, jsonify, render_template
import json
import os
from google.cloud import bigquery
from datetime import date
import threading

app = Flask(__name__)

# -------------------------------
# CONFIG
# -------------------------------
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "NVMB.geojson")

# BigQuery settings
BQ_PROJECT = os.environ.get("GCP_PROJECT") or "your-gcp-project-id"
BQ_DATASET = os.environ.get("BQ_DATASET") or "places_dataset"
BQ_TABLE = os.environ.get("BQ_TABLE") or "maharashtra_pois"

# -------------------------------
# LOAD GEOJSON
# -------------------------------
with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
    geojson_data = json.load(f)

# Initialize BigQuery client
bq_client = bigquery.Client(project=BQ_PROJECT)

# -------------------------------
# HELPER TO LOAD INTO BIGQUERY
# -------------------------------
def load_geojson_into_bq(geojson_data):
    dataset_ref = bq_client.dataset(BQ_DATASET)
    table_ref = dataset_ref.table(BQ_TABLE)

    rows_to_insert = []

    for feat in geojson_data.get("features", []):
        props = feat.get("properties", {}).copy()
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", [None, None])
        lon, lat = coords if len(coords) == 2 else (None, None)

        props["lon"] = lon
        props["lat"] = lat
        props["ingestion_date"] = date.today().isoformat()

        rows_to_insert.append(props)

    if not rows_to_insert:
        print("No rows to insert into BigQuery.")
        return False

    errors = bq_client.insert_rows_json(table_ref, rows_to_insert)

    if errors:
        print("BigQuery insert errors:", errors)
        return False

    print(f"Inserted {len(rows_to_insert)} rows into {BQ_DATASET}.{BQ_TABLE}")
    return True


# -------------------------------
# BACKGROUND LOAD (Cloud Run Safe)
# -------------------------------
def async_load_bq():
    print("Starting background BigQuery load...")
    success = load_geojson_into_bq(geojson_data)
    if success:
        print("BigQuery load completed successfully.")
    else:
        print("BigQuery load failed.")


# Start BigQuery loading in background thread
threading.Thread(target=async_load_bq, daemon=True).start()


# -------------------------------
# FLASK ENDPOINTS
# -------------------------------
@app.route("/geojson")
def geojson():
    return jsonify(geojson_data)


@app.route("/")
def map_view():
    return render_template("map.html")


# -------------------------------
# START SERVER
# -------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
