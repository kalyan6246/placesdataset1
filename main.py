# main.py
import os
import json
import traceback
from flask import Flask, request, jsonify
from shapely.geometry import shape, Point
from google.cloud import bigquery

app = Flask(__name__)

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
PROJECT_ID = os.environ.get("GCP_PROJECT") or os.environ.get("PROJECT_ID")
DATASET_ID = "places_dataset"
TABLE_ID = "places"

# Path to the GeoJSON stored inside your Git repo
REPO_GEOJSON_PATH = "data/places_polygon.geojson"


# -------------------------------------------------------------------
# READ GEOJSON FILE FROM REPO
# -------------------------------------------------------------------
def load_polygon_from_repo():
    try:
        with open(REPO_GEOJSON_PATH, "r") as f:
            gj = json.load(f)
        polygon = shape(gj["features"][0]["geometry"])
        return polygon
    except Exception as e:
        print("❌ Error reading GeoJSON from repo:", e)
        traceback.print_exc()
        return None


# -------------------------------------------------------------------
# BIGQUERY CLIENT
# -------------------------------------------------------------------
bq_client = bigquery.Client()


# -------------------------------------------------------------------
# ROUTE: TEST BIGQUERY CONNECTION
# -------------------------------------------------------------------
@app.route("/bqtest")
def bq_test():
    try:
        list(bq_client.list_datasets())
        return jsonify({"status": "connected"})
    except Exception as e:
        return jsonify({"status": "failed", "error": str(e)}), 500


# -------------------------------------------------------------------
# ROUTE: VISUALIZE GEOJSON (NO /map REQUIRED)
# -------------------------------------------------------------------
@app.route("/")
def root():
    return jsonify({"message": "Service is running"})


@app.route("/geojson")
def return_geojson():
    try:
        with open(REPO_GEOJSON_PATH, "r") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -------------------------------------------------------------------
# ROUTE: LOAD POINTS INTO BIGQUERY
# -------------------------------------------------------------------
@app.route("/load", methods=["POST"])
def load_data_to_bigquery():
    try:
        req = request.json
        if not req:
            return jsonify({"error": "Missing JSON payload"}), 400

        lat = req.get("lat")
        lon = req.get("lon")
        place_id = req.get("place_id", "unknown")

        if lat is None or lon is None:
            return jsonify({"error": "lat and lon required"}), 400

        # Load polygon
        polygon = load_polygon_from_repo()
        if polygon is None:
            return jsonify({"error": "Polygon not loaded"}), 500

        pt = Point(lon, lat)
        inside = polygon.contains(pt)

        # Prepare BigQuery row
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        table = bq_client.get_table(table_ref)

        rows_to_insert = [{
            "place_id": place_id,
            "latitude": lat,
            "longitude": lon,
            "inside_polygon": inside,
        }]

        errors = bq_client.insert_rows_json(table, rows_to_insert)

        if errors:
            print("❌ BigQuery insert errors:", errors)
            return jsonify({"status": "error", "details": errors}), 500

        return jsonify({"status": "success", "inside_polygon": inside})

    except Exception as e:
        print("❌ Exception in /load:", e)
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# -------------------------------------------------------------------
# MAIN ENTRYPOINT
# -------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
