from flask import Flask, jsonify, render_template
import json
import os
from google.cloud import bigquery
from datetime import date

app = Flask(__name__)

# -------------------------------
# ENV VARS
# -------------------------------
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "NVMB.geojson")

BQ_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET", "places_dataset")
BQ_TABLE = os.environ.get("BQ_TABLE", "maharashtra_pois")

# -------------------------------
# LOAD GEOJSON SAFELY
# -------------------------------
try:
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)
except Exception as e:
    print("ERROR loading NVMB.geojson:", e)
    geojson_data = {"type": "FeatureCollection", "features": []}


# -------------------------------
# SAFE BIGQUERY CLIENT
# -------------------------------
def get_bq():
    try:
        return bigquery.Client()
    except Exception as e:
        print("BigQuery Client Error:", e)
        return None


# -------------------------------
# CREATE DATASET + TABLE
# -------------------------------
def ensure_bq():
    client = get_bq()
    if client is None:
        return False

    # Dataset
    dataset_id = f"{BQ_PROJECT}.{BQ_DATASET}"
    dataset_ref = bigquery.Dataset(dataset_id)

    try:
        client.get_dataset(dataset_ref)
        print("Dataset exists:", dataset_id)
    except:
        client.create_dataset(dataset_ref)
        print("Created dataset:", dataset_id)

    # Table
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
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
        table_obj = bigquery.Table(table_id, schema=schema)
        client.create_table(table_obj)
        print("Created table:", table_id)

    return True


# -------------------------------
# INSERT GEOJSON POLYGONS
# -------------------------------
def insert_geojson():
    client = get_bq()
    if client is None:
        return False

    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    rows = []

    print("Preparing rows for BigQuery insert...")

    for feature in geojson_data.get("features", []):
        geom = feature.get("geometry")
        props = feature.get("properties", {})

        if not geom:
            continue

        rows.append({
            "name": props.get("name", "Polygon"),
            "type": props.get("type", "polygon"),
            "ingestion_date": date.today().isoformat(),
            "geometry": json.dumps(geom)  # GEOGRAPHY accepts JSON
        })

    print("ROWS:", rows)

    if not rows:
        print("No rows to insert.")
        return False

    errors = client.insert_rows_json(table_id, rows)

    if errors:
        print("BigQuery insert ERRORS:", errors)
        return False

    print(f"Inserted {len(rows)} polygon rows into {table_id}")
    return True


# -------------------------------
# TEST ROUTES
# -------------------------------
@app.route("/bq-test")
def bq_test():
    try:
        client = bigquery.Client()
        datasets = [d.dataset_id for d in client.list_datasets()]
        return jsonify({"bq": "connected", "datasets": datasets})
    except Exception as e:
        return jsonify({"bq": "error", "details": str(e)}), 500


@app.route("/load")
def load():
    ok1 = ensure_bq()
    ok2 = insert_geojson()
    return jsonify({"dataset_created": ok1, "inserted": ok2})


# -------------------------------
# MAIN ROUTES
# -------------------------------
@app.route("/geojson")
def get_geojson():
    return jsonify(geojson_data)


@app.route("/")
def index():
    return render_template("map.html")


# -------------------------------
# START SERVER
# -------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
