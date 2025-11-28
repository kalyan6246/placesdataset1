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
# LOAD GEOJSON SAFELY
# -------------------------------
geojson_data = None

try:
    print(f"Loading NVMB.geojson from: {GEOJSON_PATH}")
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)

    print("Loaded GeoJSON features:", len(geojson_data.get("features", [])))
except Exception as e:
    print("ERROR loading NVMB.geojson:", e)
    geojson_data = {"type": "FeatureCollection", "features": []}

# -------------------------------
# BIGQUERY HELPER
# -------------------------------
def get_bq_client():
    try:
        return bigquery.Client()
    except Exception as e:
        print("BigQuery client init error:", e)
        return None


# -------------------------------
# CREATE DATASET + TABLE IF MISSING
# -------------------------------
def ensure_bigquery():
    client = get_bq_client()
    if not client:
        return False

    dataset_id = f"{BQ_PROJECT}.{BQ_DATASET}"
    table_id = f"{dataset_id}.{BQ_TABLE}"

    # 1️⃣ Ensure dataset exists
    try:
        client.get_dataset(dataset_id)
        print("Dataset exists:", dataset_id)
    except:
        dataset = bigquery.Dataset(dataset_id)
        client.create_dataset(dataset)
        print("Created dataset:", dataset_id)

    # 2️⃣ Ensure table exists
    schema = [
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("ingestion_date", "DATE"),
        bigquery.SchemaField("geometry", "GEOGRAPHY"),
    ]

    try:
        client.get_table(table_id)
        print("Table exists:", table_id)
    except:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print("Created table:", table_id)

    return True


# -------------------------------
# INSERT POLYGONS INTO BIGQUERY
# -------------------------------
def insert_polygon_data():
    client = get_bq_client()
    if not client:
        return False

    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    rows = []

    feats = geojson_data.get("features", [])
    print("Preparing rows for BigQuery. Feature count:", len(feats))

    for feat in feats:
        geom = feat.get("geometry")
        props = feat.get("properties", {})

        if not geom:
            continue

        rows.append({
            "name": props.get("name", "Polygon"),
            "type": props.get("type", "boundary"),
            "ingestion_date": date.today().isoformat(),
            "geometry": json.dumps(geom)  # BigQuery GEOGRAPHY accepts JSON text
        })

    if not rows:
        print("No rows extracted for BigQuery insert.")
        return False

    errors = client.insert_rows_json(table_id, rows)

    if errors:
        print("BigQuery INSERT ERRORS:", errors)
        return False

    print(f"Inserted {len(rows)} polygon rows to {table_id}")
    return True


# -------------------------------
# API ROUTES
# -------------------------------
@app.route("/")
def index():
    return render_template("map.html")


@app.route("/geojson")
def serve_geojson():
    """
    This route serves NVMB.geojson to the Leaflet map.
    """
    print("Serving /geojson. Features:", len(geojson_data.get("features", [])))
    return jsonify(geojson_data)


@app.route("/load")
def load_data():
    """
    Manual trigger:
    Creates dataset + table + loads geojson into BigQuery.
    """
    ok1 = ensure_bigquery()
    ok2 = insert_polygon_data()
    return jsonify({
        "dataset_and_table_ready": ok1,
        "polygon_data_uploaded": ok2
    })


@app.route("/bq-test")
def bq_test():
    try:
        client = bigquery.Client()
        return jsonify({"status": "connected"})
    except Exception as e:
        return jsonify({"status": "error", "details": str(e)}), 500


# -------------------------------
# RUN SERVER
# -------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
