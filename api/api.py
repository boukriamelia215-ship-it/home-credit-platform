# -*- coding: utf-8 -*-
import argparse
import logging
import os
from flask import Flask, jsonify, request
from flask_jwt_extended import JWTManager, jwt_required, create_access_token
from sqlalchemy import create_engine, text

# ARGUMENTS
parser = argparse.ArgumentParser(description="API Flask + JWT")
parser.add_argument("--mysql-url",  required=True, help="URL MySQL SQLAlchemy")
parser.add_argument("--pg-url",     required=False, help="URL PostgreSQL")
parser.add_argument("--secret-key", required=True, help="Cle secrete JWT")
parser.add_argument("--port",       required=True, help="Port de l'API")
args = parser.parse_args()

# LOGS
if not os.path.exists("/app/logs"):
    os.makedirs("/app/logs")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/api.txt"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("api")

# FLASK APP
app = Flask(__name__)
app.config["JWT_SECRET_KEY"] = args.secret_key
jwt = JWTManager(app)

# CONNEXION MYSQL
engine = create_engine(args.mysql_url)
log.info("Connexion MySQL OK")

# ============================================================
# AUTH - Login pour obtenir le token JWT
# ============================================================
@app.route("/login", methods=["POST"])
def login():
    username = request.json.get("username")
    password = request.json.get("password")
    if username == "admin" and password == "admin2024":
        token = create_access_token(identity=username)
        log.info("Login OK pour {}".format(username))
        return jsonify(access_token=token), 200
    log.error("Login echoue pour {}".format(username))
    return jsonify({"error": "Identifiants incorrects"}), 401
# ============================================================
# HEALTH CHECK
# ============================================================
@app.route("/health", methods=["GET"])
def health():
    log.info("GET /health")
    return jsonify({"status": "ok", "service": "Home Credit API"}), 200

# ============================================================
# ENDPOINT 1 - Datamart Marketing
# ============================================================
@app.route("/api/marketing", methods=["GET"])
@jwt_required()
def get_marketing():
    page = int(request.args.get("page", 1))
    size = int(request.args.get("size", 100))
    offset = (page - 1) * size
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT * FROM dm_marketing LIMIT :size OFFSET :offset"
        ), {"size": size, "offset": offset})
        rows = [dict(row) for row in result]
    log.info("GET /api/marketing page={}".format(page))
    return jsonify({"page": page, "size": size, "data": rows}), 200

# ============================================================
# ENDPOINT 2 - Datamart Risque
# ============================================================
@app.route("/api/risque", methods=["GET"])
@jwt_required()
def get_risque():
    page = int(request.args.get("page", 1))
    size = int(request.args.get("size", 100))
    offset = (page - 1) * size
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT * FROM dm_risque LIMIT :size OFFSET :offset"
        ), {"size": size, "offset": offset})
        rows = [dict(row) for row in result]
    log.info("GET /api/risque page={}".format(page))
    return jsonify({"page": page, "size": size, "data": rows}), 200

# ============================================================
# ENDPOINT 3 - Datamart BI
# ============================================================
@app.route("/api/bi", methods=["GET"])
@jwt_required()
def get_bi():
    page = int(request.args.get("page", 1))
    size = int(request.args.get("size", 100))
    offset = (page - 1) * size
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT * FROM dm_bi LIMIT :size OFFSET :offset"
        ), {"size": size, "offset": offset})
        rows = [dict(row) for row in result]
    log.info("GET /api/bi page={}".format(page))
    return jsonify({"page": page, "size": size, "data": rows}), 200

# ============================================================
# LANCEMENT
# ============================================================
if __name__ == "__main__":
    log.info("Demarrage API sur le port {}".format(args.port))
    app.run(host="0.0.0.0", port=int(args.port))