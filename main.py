from flask import Flask, jsonify, render_template
import json
import os
from google.cloud import bigquery
from datetime import date

app = Flask(__name__)

GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "NVMB.geojson")

BQ_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET", "places_dataset")
BQ_TABLE = os.environ.get("BQ_TABLE", "maharashtra_pois")

# Load GeoJSON file
try:
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)
except Exception as e:
    geojson_data = {"error": str(e)}
    print("GEOJSON LOAD ERROR:", e)


def safe_bq_client():
    try:
        return bigquery.Client()
    except Exception as e:
        print("BQ CLIENT ERROR:", e)
        return None


def ensure_bq_resources():
    client = safe_bq_client()
    if client is None:
        return False

    try:
        # Dataset
        dataset_id = f"{BQ_PROJECT}.{BQ_DATASET}"
        dataset_ref = bigquery.Dataset(dataset_id)

        try:
            client.get_dataset(dataset_ref)
        except:
            client.create_dataset(dataset_ref)
            print("Created dataset", BQ_DATASET)

        # Table
        schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("ingestion_date", "DATE"),
            bigquery.SchemaField("geometry", "GEOGRAPHY"),
        ]

        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

        try:
            client.get_table(table_id)
        except:
            client.create_table(bigquery.Table(table_id, schema=schema))
            print("Created table", BQ_TABLE)

        return True

    except Exception as e:
        print("ensure_bq_resources() FAILED:", e)
        return False


def load_geojson_into_bq():
    client = safe_bq_client()
    if client is None:
        return False

    try:
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
        rows = []

        for feat in geojson_data.get("features", []):
            props = feat.get("properties", {})
            geom = feat.get("geometry")

            if not geom:
                continue

            rows.append({
                "name": props.get("name", "unknown"),
                "type": props.get("type", "polygon"),
                "ingestion_date": date.today().isoformat(),
                "geometry": json.dumps(geom),
            })

        errors = client.insert_rows_json(table_id, rows)
        if errors:
            print("BQ INSERT ERRORS:", errors)
            return False

        print("Inserted", len(rows), "rows.")
        return True

    except Exception as e:
        print("load_geojson_into_bq() FAILED:", e)
        return False


@app.route("/load")
def load_route():
    ensure_bq_resources()
    load_geojson_into_bq()
    return jsonify({"status": "OK"})


@app.route("/geojson")
def geojson():
    return jsonify(geojson_data)


@app.route("/")
def map_view():
    return render_template("map.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
