from flask import Flask, jsonify, render_template
import json
import os

app = Flask(__name__)

# Path to your GeoJSON file in the repo
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "NVMB.geojson")

# Load the GeoJSON once at startup
with open(GEOJSON_PATH, "r") as f:
    geojson_data = json.load(f)

@app.route("/geojson")
def geojson():
    return jsonify(geojson_data)

@app.route("/")
def map_view():
    return render_template("map.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
