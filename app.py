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
from werkzeug.security import check_password_hash
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# =====================================
# Cargar variables de entorno
# =====================================
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "minminas_app")
MONGO_COLECCION = os.getenv("MONGO_COLECCION", "usuarios")

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_USER = os.getenv("ES_USER")
ES_PASS = os.getenv("ES_PASS")
ES_INDEX = os.getenv("ES_INDEX", "normatividad_minminas")

# =====================================
# Flask
# =====================================
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-cambia-esto")


# =====================================
# Conexión MongoDB
# =====================================
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
usuarios_col = db[MONGO_COLECCION]
print(f"[OK] MongoDB conectado a {MONGO_DB} / colección {MONGO_COLECCION}")


# =====================================
# Conexión Elasticsearch
# =====================================
if ES_USER and ES_PASS:
    es = Elasticsearch(ES_HOST, basic_auth=(ES_USER, ES_PASS), verify_certs=True)
else:
    es = Elasticsearch(ES_HOST, verify_certs=True)

try:
    info = es.info()
    cluster_name = info.get("cluster_name", "OK")
    print(f"[OK] Elasticsearch conectado: {cluster_name}")
except Exception as e:
    print("[ERROR] No se pudo conectar a Elasticsearch:", e)


# =====================================
# RUTA PRINCIPAL: buscador
# =====================================
@app.route("/", methods=["GET"])
def home():
    q = request.args.get("q", "").strip()
    resultados = []
    total = 0

    if q:
        # Consulta de texto completo
        es_query = {
            "multi_match": {
                "query": q,
                "fields": [
                    "titulo^3",
                    "titulo_norma^3",
                    "resumen^2",
                    "texto",
                ],
            }
        }

        resp = es.search(index=ES_INDEX, query=es_query, size=30)
        total = resp["hits"]["total"]["value"]

        for hit in resp["hits"]["hits"]:
            src = hit.get("_source", {}) or {}

            # 🔎 Ajusta aquí los nombres de campos reales de tu índice
            titulo = (
                src.get("titulo")
                or src.get("titulo_norma")
                or src.get("title")
                or "Sin título"
            )

            entidad = (
                src.get("entidad")
                or src.get("entidad_emisora")
                or src.get("entity")
                or "N/D"
            )

            anio = src.get("anio") or src.get("ano") or src.get("year") or "N/D"

            tipo = src.get("tipo") or src.get("tipo_norma") or src.get("type") or "N/D"

            resultados.append(
                {
                    "titulo": titulo,
                    "entidad": entidad,
                    "anio": anio,
                    "tipo": tipo,
                    "score": round(hit.get("_score", 0.0), 2),
                }
            )

    return render_template("index.html", q=q, resultados=resultados, total=total)


# =====================================
# ACERCA DE
# =====================================
@app.route("/about")
def about():
    return render_template("about.html")


# =====================================
# LOGIN
# =====================================
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        # name="login" y name="password" en el formulario
        login_input = (request.form.get("login") or "").strip()
        password = (request.form.get("password") or "").strip()

        if not login_input or not password:
            flash("Debes ingresar usuario/correo y contraseña.", "danger")
            return render_template("login.html")

        # Buscar por username o por email
        user = usuarios_col.find_one(
            {"$or": [{"username": login_input}, {"email": login_input}]}
        )

        if not user:
            flash("Usuario o contraseña incorrectos.", "danger")
            return render_template("login.html")

        if not check_password_hash(user["password"], password):
            flash("Usuario o contraseña incorrectos.", "danger")
            return render_template("login.html")

        # Login correcto
        session.clear()
        session["user_id"] = str(user["_id"])
        session["username"] = user.get("username")
        session["rol"] = user.get("rol", "usuario")

        flash(f"Bienvenido, {user.get('username')}", "success")
        # Por ahora, después de loguear volvemos al buscador
        return redirect(url_for("home"))

    # GET
    return render_template("login.html")


# =====================================
# LOGOUT
# =====================================
@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada correctamente.", "info")
    return redirect(url_for("home"))


# =====================================
# MAIN (solo local)
# =====================================
if __name__ == "__main__":
    # En Render no usa debug, pero local sí
    app.run(debug=True, host="0.0.0.0", port=5000)
