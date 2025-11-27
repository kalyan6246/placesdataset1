import os
import json
import time
from datetime import datetime, date
import requests
from shapely.geometry import shape, Point
from google.cloud import storage, secretmanager, bigquery
from flask import Flask, jsonify
import threading

# -------------------------------
# CONFIGURATION
# -------------------------------
PROJECT_ID = os.environ.get('GCP_PROJECT') or os.environ.get('PROJECT_ID')
BUCKET_NAME = os.environ.get('PLACES_BUCKET') or 'PROJECT_BUCKET'
GCS_PREFIX = os.environ.get('GCS_PREFIX') or 'places_exports'
MAHARAJSHA_FILE = os.environ.get('MAHARAJ_SHA_GEOJSON') or 'mvmu/NVMB.geojson'
SECRET_NAME = os.environ.get('API_KEY_SECRET') or f"projects/{PROJECT_ID}/secrets/PLACES_API_KEY/versions/1"

GRID_STEP_DEG = float(os.environ.get('GRID_STEP_DEG') or 0.0005)
RADIUS_METERS = int(os.environ.get('RADIUS_METERS') or 300)

NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

BQ_DATASET = os.environ.get('BQ_DATASET') or 'places_dataset'
BQ_TABLE = os.environ.get('BQ_TABLE') or 'maharashtra_pois'

# Google Clients
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()
bq_client = bigquery.Client()

# -------------------------------
# SECRET MANAGER
# -------------------------------
def get_api_key():
    print("Fetching API key from Secret Manager...")
    response = secret_client.access_secret_version(request={"name": SECRET_NAME})
    return response.payload.data.decode("utf-8").strip()

# -------------------------------
# LOAD POLYGON
# -------------------------------
def load_polygon_from_gcs(bucket_name, blob_path):
    print(f"Loading polygon from GCS: gs://{bucket_name}/{blob_path}")

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    data = blob.download_as_text(encoding="utf-8-sig")
    gj = json.loads(data)

    if gj.get('type') == 'FeatureCollection':
        geom = gj['features'][0]['geometry']
    elif gj.get('type') == 'Feature':
        geom = gj['geometry']
    else:
        geom = gj

    polygon = shape(geom)
    print("Polygon bounds:", polygon.bounds)
    return polygon

# -------------------------------
# GRID GENERATOR
# -------------------------------
def generate_grid_points_within_polygon(polygon, step_deg):
    print(f"Generating grid with step {step_deg} degrees...")

    minx, miny, maxx, maxy = polygon.bounds
    lat = miny
    pts = []

    while lat <= maxy:
        lon = minx
        while lon <= maxx:
            p = Point(lon, lat)
            if polygon.covers(p):
                pts.append((lat, lon))
            lon += step_deg
        lat += step_deg

    print(f"Generated {len(pts)} grid points.")
    return pts

# -------------------------------
# GOOGLE PLACES CALL
# -------------------------------
def places_nearby(api_key, lat, lon, radius, place_type=None):
    params = {
        'key': api_key,
        'location': f"{lat},{lon}",
        'radius': radius
    }
    if place_type:
        params['type'] = place_type

    resp = requests.get(NEARBY_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

# -------------------------------
# FORMAT FEATURE
# -------------------------------
def feature_from_place(place):
    loc = place.get('geometry', {}).get('location', {})
    if 'lat' not in loc or 'lng' not in loc:
        return None

    props = {
        'place_id': place.get('place_id'),
        'name': place.get('name'),
        'types': place.get('types'),
        'vicinity': place.get('vicinity') or place.get('formatted_address'),
        'rating': place.get('rating'),
        'user_ratings_total': place.get('user_ratings_total'),
        'plus_code': place.get('plus_code', {}).get('global_code') if place.get('plus_code') else None,
        'collected_at': datetime.utcnow().isoformat() + 'Z'
    }

    geom = {"type": "Point", "coordinates": [loc['lng'], loc['lat']]}

    return {"type": "Feature", "geometry": geom, "properties": props}

# -------------------------------
# UPLOAD TO GCS
# -------------------------------
def upload_geojson_to_gcs(features, bucket_name, prefix):
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{prefix}/places_{ts}.geojson"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    payload = {"type": "FeatureCollection", "features": features}
    blob.upload_from_string(json.dumps(payload), content_type="application/geo+json")

    gcs_uri = f"gs://{bucket_name}/{filename}"
    print("Uploaded GeoJSON to:", gcs_uri)

    return gcs_uri

# -------------------------------
# LOAD INTO BIGQUERY
# -------------------------------
def load_geojson_into_bq(gcs_uri, dataset_id, table_id):
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    bucket_name, blob_path = gcs_uri.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    geojson_text = blob.download_as_text(encoding="utf-8-sig")
    gj = json.loads(geojson_text)

    ndjson_lines = []
    for feat in gj["features"]:
        props = feat["properties"]
        lon, lat = feat["geometry"]["coordinates"]

        props["lon"] = lon
        props["lat"] = lat
        props["ingestion_date"] = date.today().isoformat()

        ndjson_lines.append(json.dumps(props))

    temp_path = f"{blob_path}.ndjson"
    bucket.blob(temp_path).upload_from_string("\n".join(ndjson_lines))

    temp_uri = f"gs://{bucket_name}/{temp_path}"
    print("Loading NDJSON into BigQuery:", temp_uri)

    load_job = bq_client.load_table_from_uri(temp_uri, table_ref, job_config=job_config)
    load_job.result()

    print("BigQuery load completed:", load_job.job_id)
    return load_job

# -------------------------------
# MAIN JOB LOGIC
# -------------------------------
def run_job():
    print("===== PLACES EXTRACTION JOB STARTED =====")

    api_key = get_api_key()
    polygon = load_polygon_from_gcs(BUCKET_NAME, MAHARAJSHA_FILE)

    grid = generate_grid_points_within_polygon(polygon, GRID_STEP_DEG)

    if len(grid) == 0:
        print("ERROR: No grid points generated. Try smaller GRID_STEP_DEG.")
        print("Polygon bounds:", polygon.bounds)
        return

    features = []
    seen = set()
    total_api_calls = 0

    for lat, lon in grid:
        resp = places_nearby(api_key, lat, lon, RADIUS_METERS)
        total_api_calls += 1

        for place in resp.get("results", []):
            pid = place.get("place_id")
            if pid in seen:
                continue
            seen.add(pid)

            feat = feature_from_place(place)
            if feat:
                features.append(feat)

        next_page = resp.get("next_page_token")
        retries = 0
        while next_page:
            time.sleep(2)
            resp2 = requests.get(
                NEARBY_URL,
                params={"key": api_key, "pagetoken": next_page},
                timeout=30
            ).json()

            total_api_calls += 1

            for place in resp2.get("results", []):
                pid = place.get("place_id")
                if pid in seen:
                    continue
                seen.add(pid)

                feat = feature_from_place(place)
                if feat:
                    features.append(feat)

            next_page = resp2.get("next_page_token")
            retries += 1
            if retries > 5:
                break

    print(f"Total POIs: {len(features)}, API calls: {total_api_calls}")

    gcs_uri = upload_geojson_to_gcs(features, BUCKET_NAME, GCS_PREFIX)
    load_geojson_into_bq(gcs_uri, BQ_DATASET, BQ_TABLE)

    print("===== JOB COMPLETED SUCCESSFULLY =====")

# -------------------------------
# CLOUD RUN SERVICE ENTRYPOINT
# -------------------------------
app = Flask(__name__)

@app.get("/")
def home():
    return {"status": "running", "message": "Places ingestion service"}

@app.get("/run")
def trigger_job():
    thread = threading.Thread(target=run_job)
    thread.start()
    return jsonify({"status": "Job started"}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
