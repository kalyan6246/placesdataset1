from flask import Flask, jsonify, render_template
import json
import os
from google.cloud import bigquery
from datetime import date

app = Flask(__name__)

# -------------------------------
# CONFIG
# -------------------------------
# GeoJSON file is in the repo root (same folder as main.py)
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "NVMB.geojson")

# BigQuery settings
BQ_PROJECT = os.environ.get("GCP_PROJECT") or os.environ.get("PROJECT_ID")
BQ_DATASET = os.environ.get("BQ_DATASET") or "places_dataset"
BQ_TABLE = os.environ.get("BQ_TABLE") or "maharashtra_pois"

# -------------------------------
# LOAD GEOJSON
# -------------------------------
def load_geojson():
    try:
        with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        return {"error": str(e)}

geojson_data = load_geojson()

# -------------------------------
# BIGQUERY CLIENT
# -------------------------------
bq_client = bigquery.Client(project=BQ_PROJECT)

# -------------------------------
# FUNCTION: Load into BigQuery
# -------------------------------
def load_geojson_into_bq():
    # If GeoJSON failed to load
    if "error" in geojson_data:
        return {"status": "error", "details": geojson_data["error"]}

    dataset_ref = bq_client.dataset(BQ_DATASET)
    table_ref = dataset_ref.table(BQ_TABLE)

    rows_to_insert = []
    for feat in geojson_data.get("features", []):
        props = dict(feat.get("properties", {}))
        geom = feat.get("geometry", {})

        # Extract coordinates (for polygon use centroid)
        lon, lat = None, None

        if geom.get("type") == "Polygon":
            try:
                # Take first coordinate pair of first ring
                lon, lat = geom["coordinates"][0][0]
            except:
                lon, lat = None, None

        elif geom.get("type") == "Point":
            try:
                lon, lat = geom["coordinates"]
            except:
                lon, lat = None, None

        props["lon"] = lon
        props["lat"] = lat
        props["ingestion_date"] = date.today().isoformat()

        rows_to_insert.append(props)

    if not rows_to_insert:
        return {"status": "no_data", "details": "No features found in GeoJSON"}

    # Insert rows
    errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        return {"status": "bq_error", "details": errors}

    return {"status": "success", "inserted": len(rows_to_insert)}

# -------------------------------
# ROUTES
# -------------------------------

@app.route("/geojson")
def geojson():
    """Return GeoJSON content"""
    return jsonify(geojson_data)

@app.route("/load")
def load():
    """Load data from GeoJSON into BigQuery"""
    result = load_geojson_into_bq()
    return jsonify(result)

@app.route("/bqtest")
def bq_test():
    """Check BigQuery connection"""
    try:
        list(bq_client.list_datasets())
        return jsonify({"status": "connected"})
    except Exception as e:
        return jsonify({"status": "error", "details": str(e)})

@app.route("/")
def map_view():
    """Render the HTML map"""
    return render_template("map.html")

# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
