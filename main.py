# main.py
import os
import json
import time
from datetime import datetime, date
from flask import Flask, request, jsonify
import requests
from shapely.geometry import shape, Point
from google.cloud import storage, secretmanager, bigquery
 
# -----------------------------------------------------------------------------
# ENV CONFIGURATION
# -----------------------------------------------------------------------------
 
PROJECT_ID = os.environ.get("GCP_PROJECT")
BUCKET_NAME = os.environ.get("PLACES_BUCKET")       # ex: mvmu
GCS_PREFIX = os.environ.get("GCS_PREFIX")           # ex: places_exports
MAHARAJSHA_FILE = os.environ.get("MAHARAJ_SHA_GEOJSON")  # ex: NVMB.geojson
SECRET_NAME = os.environ.get("API_KEY_SECRET")
 
GRID_STEP_DEG = float(os.environ.get("GRID_STEP_DEG") or 0.0005)
RADIUS_METERS = int(os.environ.get("RADIUS_METERS") or 300)
BQ_DATASET = os.environ.get("BQ_DATASET")           # ex: places_dataset
BQ_TABLE = os.environ.get("BQ_TABLE")               # ex: maharashtra_pois
 
NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
 
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()
bq_client = bigquery.Client()
 
app = Flask(__name__)
 
 
# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
 
def get_api_key():
    """Fetch API key from Secret Manager"""
    response = secret_client.access_secret_version(request={"name": SECRET_NAME})
    return response.payload.data.decode("utf-8").strip()
 
 
def load_polygon_from_gcs(bucket_name, blob_path):
    """Load polygon + raw geojson"""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
 
    data = blob.download_as_text(encoding="utf-8-sig")
    gj = json.loads(data)
 
    if gj.get("type") == "FeatureCollection":
        geom = gj["features"][0]["geometry"]
    elif gj.get("type") == "Feature":
        geom = gj["geometry"]
    else:
        geom = gj
 
    return shape(geom), gj
 
 
def generate_grid_points_within_polygon(polygon, step_deg):
    """Generate grid points inside the polygon"""
    minx, miny, maxx, maxy = polygon.bounds
    pts = []
 
    lat = miny
    while lat <= maxy:
        lon = minx
        while lon <= maxx:
            point = Point(lon, lat)
            if polygon.covers(point):        # IMPORTANT
                pts.append((lat, lon))
            lon += step_deg
        lat += step_deg
 
    return pts
 
 
def places_nearby(api_key, lat, lon, radius, place_type=None):
    params = {
        "key": api_key,
        "location": f"{lat},{lon}",
        "radius": radius
    }
    if place_type:
        params["type"] = place_type
 
    r = requests.get(NEARBY_URL, params=params, timeout=25)
    r.raise_for_status()
    return r.json()
 
 
def feature_from_place(place):
    loc = place.get("geometry", {}).get("location", {})
    if "lat" not in loc or "lng" not in loc:
        return None
 
    props = {
        "place_id": place.get("place_id"),
        "name": place.get("name"),
        "types": place.get("types"),
        "vicinity": place.get("vicinity") or place.get("formatted_address"),
        "rating": place.get("rating"),
        "user_ratings_total": place.get("user_ratings_total"),
        "plus_code": place.get("plus_code", {}).get("global_code"),
        "collected_at": datetime.utcnow().isoformat() + "Z"
    }
 
    geom = {
        "type": "Point",
        "coordinates": [loc["lng"], loc["lat"]]
    }
 
    return {"type": "Feature", "geometry": geom, "properties": props}
 
 
def upload_geojson_to_gcs(features, bucket_name, prefix):
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{prefix}/places_{ts}.geojson"
 
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
 
    payload = {"type": "FeatureCollection", "features": features}
 
    blob.upload_from_string(json.dumps(payload), content_type="application/geo+json")
 
    return f"gs://{bucket_name}/{filename}", filename
 
 
def load_geojson_into_bq(gcs_uri, dataset_id, table_id):
    """Convert GeoJSON → NDJSON → BQ"""
    bucket_name, path = gcs_uri.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(path)
 
    raw = json.loads(blob.download_as_text())
 
    ndjson_list = []
    for feat in raw["features"]:
        props = feat["properties"]
        lon, lat = feat["geometry"]["coordinates"]
        props["lon"] = lon
        props["lat"] = lat
        props["ingestion_date"] = date.today().isoformat()
        ndjson_list.append(json.dumps(props))
 
    temp_path = f"{path}.ndjson"
    temp_blob = bucket.blob(temp_path)
    temp_blob.upload_from_string("\n".join(ndjson_list))
 
    load_job = bq_client.load_table_from_uri(
        f"gs://{bucket_name}/{temp_path}",
        bq_client.dataset(dataset_id).table(table_id),
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
            autodetect=True
        )
    )
    load_job.result()
    return load_job
 
 
# -----------------------------------------------------------------------------
# UI MAP ENDPOINTS
# -----------------------------------------------------------------------------
 
@app.route("/polygon")
def api_polygon():
    try:
        _, raw = load_polygon_from_gcs(BUCKET_NAME, MAHARAJSHA_FILE)
        return app.response_class(
            response=json.dumps(raw),
            mimetype="application/geo+json"
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
 
@app.route("/geojson")
def api_latest_geojson():
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=GCS_PREFIX))
 
    if not blobs:
        return jsonify({"error": "No files found"}), 404
 
    latest = max(blobs, key=lambda b: b.time_created)
    data = latest.download_as_text()
 
    return app.response_class(response=data, mimetype="application/geo+json")
 
 
@app.route("/latest-file")
def api_latest_file():
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=GCS_PREFIX))
    if not blobs:
        return jsonify({"error": "No files found"}), 404
 
    latest = max(blobs, key=lambda b: b.time_created)
 
    return jsonify({
        "file": latest.name,
        "gs_uri": f"gs://{BUCKET_NAME}/{latest.name}"
    })
 
 
# -----------------------------------------------------------------------------
# MAIN PLACES INGESTION (TRIGGERED API)
# -----------------------------------------------------------------------------
 
@app.route("/", methods=["GET"])
def health():
    return jsonify({"message": "Places ingestion service", "status": "running"})
 
 
@app.route("/run", methods=["POST"])
def run_ingestion():
    """
    This endpoint runs complete logic:
    polygon → grid → nearby → GCS → BigQuery
    """
 
    try:
        api_key = get_api_key()
    except Exception as e:
        return jsonify({"error": "Failed to fetch API key", "details": str(e)}), 500
 
    try:
        polygon, _ = load_polygon_from_gcs(BUCKET_NAME, MAHARAJSHA_FILE)
    except Exception as e:
        return jsonify({"error": "Polygon load failed", "details": str(e)}), 500
 
    grid = generate_grid_points_within_polygon(polygon, GRID_STEP_DEG)
    if not grid:
        return jsonify({"error": "Grid empty"}), 400
 
    features = []
    seen = set()
    total_calls = 0
 
    for lat, lon in grid:
        try:
            resp = places_nearby(api_key, lat, lon, RADIUS_METERS)
        except Exception:
            continue
 
        total_calls += 1
 
        for place in resp.get("results", []):
            pid = place.get("place_id")
            if not pid or pid in seen:
                continue
 
            point = Point(
                place["geometry"]["location"]["lng"],
                place["geometry"]["location"]["lat"]
            )
 
            if polygon.covers(point):
                feat = feature_from_place(place)
                if feat:
                    features.append(feat)
                    seen.add(pid)
 
    # Upload GeoJSON
    gcs_uri, file = upload_geojson_to_gcs(features, BUCKET_NAME, GCS_PREFIX)
 
    # BigQuery ingestion
    job = load_geojson_into_bq(gcs_uri, BQ_DATASET, BQ_TABLE)
 
    return jsonify({
        "message": "Success",
        "gcs_uri": gcs_uri,
        "file": file,
        "total_pois": len(features),
        "api_calls": total_calls,
        "bq_job": job.job_id
    })
 
 
# -----------------------------------------------------------------------------
 
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
