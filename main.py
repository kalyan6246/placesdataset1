# main.py
import os
import json
import time
from datetime import datetime, date
from flask import Flask, request, jsonify
import requests
from shapely.geometry import shape, Point
from google.cloud import storage, secretmanager, bigquery
 
# Config via env vars
PROJECT_ID = os.environ.get('GCP_PROJECT')
BUCKET_NAME = os.environ.get('PLACES_BUCKET')
GCS_PREFIX = os.environ.get('GCS_PREFIX') or "places_exports"
MAHARAJSHA_FILE = os.environ.get('MAHARAJ_SHA_GEOJSON')
SECRET_NAME = os.environ.get('API_KEY_SECRET')
GRID_STEP_DEG = float(os.environ.get('GRID_STEP_DEG') or 0.1)  
RADIUS_METERS = int(os.environ.get('RADIUS_METERS') or 35000)
NEARBY_URL = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json'
BQ_DATASET = os.environ.get('BQ_DATASET')
BQ_TABLE = os.environ.get('BQ_TABLE')
 
# Clients
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()
bq_client = bigquery.Client()
 
app = Flask(__name__)
 
def get_api_key():
    response = secret_client.access_secret_version(request={"name": SECRET_NAME})
    return response.payload.data.decode("utf-8").strip()
 
def load_polygon_from_gcs(bucket_name, blob_path):
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
    return shape(geom)
 
def generate_grid_points_within_polygon(polygon, step_deg):
    minx, miny, maxx, maxy = polygon.bounds
    pts = []
    lat = miny
    while lat <= maxy:
        lon = minx
        while lon <= maxx:
            p = Point(lon, lat)
            if polygon.covers(p):
                pts.append((lat, lon))
            lon += step_deg
        lat += step_deg
    return pts
 
def places_nearby(api_key, lat, lon, radius):
    params = {
        'key': api_key,
        'location': f'{lat},{lon}',
        'radius': radius
    }
    resp = requests.get(NEARBY_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()
 
def feature_from_place(place):
    loc = place.get('geometry', {}).get('location', {})
    if 'lat' not in loc or 'lng' not in loc:
        return None
    props = {
        'place_id': place.get('place_id'),
        'name': place.get('name'),
        'types': place.get('types'),
        'vicinity': place.get('vicinity'),
        'rating': place.get('rating'),
        'user_ratings_total': place.get('user_ratings_total'),
        'collected_at': datetime.utcnow().isoformat() + 'Z'
    }
    geom = {
        'type': 'Point',
        'coordinates': [loc['lng'], loc['lat']]
    }
    return {'type': 'Feature', 'geometry': geom, 'properties': props}
 
def upload_geojson_to_gcs(features):
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    filename = f'{GCS_PREFIX}/places_{ts}.geojson'
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)
 
    blob.upload_from_string(json.dumps({
        'type': 'FeatureCollection',
        'features': features
    }), content_type='application/geo+json')
 
    return f'gs://{BUCKET_NAME}/{filename}', filename
 
@app.route('/run', methods=['POST'])
def run_collection_http():
    try:
        api_key = get_api_key()
        polygon = load_polygon_from_gcs(BUCKET_NAME, MAHARAJSHA_FILE)
 
        grid = generate_grid_points_within_polygon(polygon, GRID_STEP_DEG)
        if not grid:
            return jsonify({"error": "Polygon too small or step size too big"}), 400
 
        seen = set()
        features = []
        api_calls = 0
 
        for (lat, lon) in grid:
            resp = places_nearby(api_key, lat, lon, RADIUS_METERS)
            api_calls += 1
 
            for place in resp.get('results', []):
                pid = place.get('place_id')
                if pid and pid not in seen:
                    f = feature_from_place(place)
                    if f:
                        seen.add(pid)
                        features.append(f)
 
        gcs_uri, filename = upload_geojson_to_gcs(features)
 
        return jsonify({
            "message": "Success",
            "gcs_uri": gcs_uri,
            "file": filename,
            "total_pois": len(features),
            "api_calls": api_calls
        })
 
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
 
# Cloud Run needs this
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
