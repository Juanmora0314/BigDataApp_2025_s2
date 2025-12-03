import os

from dotenv import load_dotenv
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
    current_app,
)
from pymongo import MongoClient
from werkzeug.security import generate_password_hash, check_password_hash
from elasticsearch import Elasticsearch

# ====================== Carga de variables de entorno ======================

load_dotenv()

# ---------------------- Flask ----------------------
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev_key_insegura_cambia_esto")

# ---------------------- MongoDB ----------------------
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "minminas_app")
MONGO_COLECCION = os.getenv("MONGO_COLECCION", "usuarios")

mongo_client = None
mongo_db = None
usuarios_col = None

if MONGO_URI:
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        mongo_db = mongo_client[MONGO_DB]
        mongo_db.command("ping")
        usuarios_col = mongo_db[MONGO_COLECCION]
        print(f"[OK] MongoDB conectado a {MONGO_DB} / colección {MONGO_COLECCION}")
    except Exception as e:
        print(f"[ERROR] Conectando a MongoDB: {e}")
else:
    print("[WARN] No hay MONGO_URI definida. MongoDB desactivado.")

# ---------------------- Elasticsearch ----------------------
ELASTIC_CLOUD_ID = os.getenv("ELASTIC_CLOUD_ID")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")
ELASTIC_INDEX_DEFAULT = os.getenv("ELASTIC_INDEX_DEFAULT", "minminas-normatividad")

elastic_client = None
elastic_configured = False

if ELASTIC_CLOUD_ID and ELASTIC_API_KEY:
    try:
        elastic_client = Elasticsearch(
            cloud_id=ELASTIC_CLOUD_ID,
            api_key=ELASTIC_API_KEY,
        )
        info = elastic_client.info()
        print(f"[OK] Elasticsearch conectado: {info['cluster_name']}")
        elastic_configured = True
    except Exception as e:
        print(f"[ERROR] Conectando a Elasticsearch: {e}")
else:
    print("[WARN] No hay ELASTIC_CLOUD_ID o ELASTIC_API_KEY. Buscador desactivado.")

app.config["ELASTIC_CLIENT"] = elastic_client
app.config["ELASTIC_CONFIGURED"] = elastic_configured
app.config["ELASTIC_INDEX_DEFAULT"] = ELASTIC_INDEX_DEFAULT

# =====================================================================
#                               RUTAS
# =====================================================================

# ---------------------- Landing / inicio ----------------------
@app.route("/")
def index():
    return render_template("index.html")


# ---------------------- Acerca de mí ----------------------
@app.route("/about")
def about():
    return render_template("about.html")


# ---------------------- Login / Logout ----------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        identifier = request.form.get("username") or request.form.get("email")
        password = request.form.get("password")

        if not usuarios_col:
            flash("Base de datos de usuarios no disponible.", "danger")
            return redirect(url_for("login"))

        user = usuarios_col.find_one(
            {"$or": [{"username": identifier}, {"email": identifier}]}
        )

        if user and check_password_hash(user["password"], password):
            session["user_id"] = str(user["_id"])
            session["username"] = user.get("username")
            session["rol"] = user.get("rol", "usuario")
            flash("Inicio de sesión correcto.", "success")
            return redirect(url_for("dashboard"))
        else:
            flash("Usuario o contraseña incorrectos.", "danger")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada.", "info")
    return redirect(url_for("index"))


# ---------------------- Dashboard sencillo ----------------------
@app.route("/dashboard")
def dashboard():
    if "user_id" not in session:
        return redirect(url_for("login"))
    # Aquí puedes cambiar por admin.html o el template que tú uses
    return render_template("admin.html")


# ---------------------- Buscador (Elasticsearch) ----------------------
@app.route("/buscador")
def buscador():
    q = request.args.get("q", "").strip()
    elastic_configured_flag = current_app.config.get("ELASTIC_CONFIGURED", False)
    resultados = []
    error_msg = None

    if q and elastic_configured_flag:
        es = current_app.config["ELASTIC_CLIENT"]
        index_name = current_app.config["ELASTIC_INDEX_DEFAULT"]

        try:
            resp = es.search(
                index=index_name,
                query={
                    "multi_match": {
                        "query": q,
                        "fields": ["titulo^3", "descripcion^2", "texto"],
                        "type": "best_fields",
                    }
                },
                size=20,
            )

            resultados = [
                {
                    "id": hit["_id"],
                    "score": hit["_score"],
                    "titulo": hit["_source"].get("titulo"),
                    "entidad": hit["_source"].get("entidad"),
                    "anio": hit["_source"].get("anio"),
                    "tipo": hit["_source"].get("tipo"),
                }
                for hit in resp["hits"]["hits"]
            ]
        except Exception as e:
            error_msg = f"Error al consultar Elasticsearch: {e}"

    elif q and not elastic_configured_flag:
        error_msg = "El buscador no está configurado (Elasticsearch sin URL)."

    return render_template(
        "buscador.html",
        query=q,
        resultados=resultados,
        elastic_configured=elastic_configured_flag,
        error_msg=error_msg,
    )


# ---------------------- Punto de entrada ----------------------
if __name__ == "__main__":
    # En Render no se usa este run, pero en local sí.
    app.run(debug=True)
