import os
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
)
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from passlib.hash import bcrypt

# -----------------------------------------------------------------------------
# Configuración básica de Flask
# -----------------------------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-minminas-2025")

# -----------------------------------------------------------------------------
# MongoDB
# -----------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "minminas_app")

if not MONGO_URI:
    raise RuntimeError("Falta la variable de entorno MONGO_URI")

try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    usuarios_col = db["usuarios"]
    print("[OK] MongoDB conectado a minminas_app / colección usuarios")
except Exception as e:
    print("[ERROR] No se pudo conectar a MongoDB:", e)
    usuarios_col = None

# -----------------------------------------------------------------------------
# Elasticsearch – usa tu clúster de Elastic Cloud
# -----------------------------------------------------------------------------
# 👉 Si quieres, pon estas dos cosas en variables de entorno ES_URL y ES_API_KEY.
ES_URL = os.getenv(
    "ES_URL",
    "https://f5a223ee7900463db5fdfe34f348f883.us-central1.gcp.cloud.es.io:443",
)
ES_API_KEY = os.getenv(
    "ES_API_KEY",
    "RThQXzY1b0JRd3FzdUVaQXU3aHk6RC1Kc2lhcmp1UFVkcllWdy0tLVZMQQ==",
)

# Nombre del índice (ajústalo al que usaste al cargar los datos)
ES_INDEX = os.getenv("ES_INDEX", "minminas_normas")

es = None
try:
    es = Elasticsearch(ES_URL, api_key=ES_API_KEY)
    info = es.info()
    print(f"[OK] Elasticsearch conectado: {info.get('cluster_name', 'cluster')}")
except Exception as e:
    es = None
    print("[ERROR] No se pudo conectar a Elasticsearch:", e)

# -----------------------------------------------------------------------------
# Rutas
# -----------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def home():
    q = request.args.get("q", "").strip()
    resultados = []
    total = 0

    if q:
        if es is None:
            flash(
                "El buscador no está disponible en este momento "
                "(sin conexión a Elasticsearch).",
                "warning",
            )
        else:
            es_query = {
                "multi_match": {
                    "query": q,
                    "fields": [
                        "titulo^3",
                        "tema^2",
                        "descripcion",
                        "entidad",
                        "tipo_norma",
                    ],
                }
            }
            try:
                resp = es.search(index=ES_INDEX, query=es_query, size=30)
                total = resp["hits"]["total"]["value"]
                for h in resp["hits"]["hits"]:
                    src = h["_source"]
                    resultados.append(
                        {
                            "titulo": src.get("titulo"),
                            "entidad": src.get("entidad"),
                            "anio": src.get("anio"),
                            "tipo_norma": src.get("tipo_norma"),
                            "url": src.get("url_pdf") or src.get("url"),
                            "score": round(h["_score"], 2),
                        }
                    )
            except Exception as e:
                app.logger.error(f"Error al buscar en Elasticsearch: {e}")
                flash(
                    "Hubo un error al consultar el buscador. "
                    "Intenta de nuevo más tarde.",
                    "danger",
                )

    return render_template(
        "home.html",
        active="home",
        query=q,
        resultados=resultados,
        total=total,
    )


@app.route("/about")
def about():
    return render_template("about.html", active="about")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username_or_email = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        # Búsqueda por usuario o correo
        user = None
        if usuarios_col is not None and username_or_email:
            user = usuarios_col.find_one(
                {
                    "$or": [
                        {"username": username_or_email},
                        {"correo": username_or_email},
                    ]
                }
            )

        # IMPORTANTE:
        # Ajusta el campo de contraseña según lo que tengas en Mongo:
        # 1) Si guardaste hash bcrypt en "password_hash", deja bcrypt.verify.
        # 2) Si guardaste texto plano en "password", cambia a (user["password"] == password).
        cred_ok = False
        if user:
            if "password_hash" in user:
                cred_ok = bcrypt.verify(password, user["password_hash"])
            elif "password" in user:
                cred_ok = user["password"] == password

        if user and cred_ok:
            session["user_id"] = str(user["_id"])
            session["user_name"] = user.get("nombre") or user.get("username")
            flash(f"Bienvenido, {session['user_name']}.", "success")
            return redirect(url_for("home"))
        else:
            flash("Usuario o contraseña incorrectos.", "danger")

    # Listado de usuarios para mostrar en pantalla (solo lectura)
    usuarios = []
    if usuarios_col is not None:
        usuarios = list(
            usuarios_col.find(
                {}, {"_id": 0, "nombre": 1, "username": 1, "correo": 1, "rol": 1}
            ).sort("nombre", 1)
        )

    return render_template("login.html", active="login", usuarios=usuarios)


# -----------------------------------------------------------------------------
# Main (para correr localmente)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True)
