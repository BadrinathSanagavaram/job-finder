"""
Flask REST API — serves dashboard data from BigQuery.
Run locally: python api.py
"""
from flask import Flask, jsonify
from flask_cors import CORS
from database import query_dashboard

app = Flask(__name__)
CORS(app)


@app.route("/api/dashboard", methods=["GET"])
def dashboard():
    try:
        data = query_dashboard()
        return jsonify({"status": "ok", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
