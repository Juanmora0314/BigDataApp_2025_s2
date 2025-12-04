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

# =====================================================================
#                   Carga de variables de entorno
# =====================================================================

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
#                      Utilidades auxiliares
# =====================================================================

def _first_existing(source: dict, keys, default=None):
    """
    Devuelve el primer valor no vacío que encuentre en source
    para las llaves indicadas.
    """
    if not source:
        return default
    for k in keys:
        valor = source.get(k)
        if valor not in (None, "", "N/D"):
            return valor
    return default


@app.context_processor
def inject_current_user():
    """
    Inyecta current_user en todas las plantillas.
    """
    if "user_id" in session:
        return {
            "current_user": {
                "id": session.get("user_id"),
                "username": session.get("username"),
                "rol": session.get("rol", "usuario"),
            }
        }
    return {"current_user": None}


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
    """
    Inicio de sesión:
      - Busca por username o email.
      - Verifica password hasheado.
      - Guarda info básica en session.
      - Redirige al panel de usuarios.
    """
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
            session["user_id"] = str(user.get("_id"))
            session["username"] = user.get("username")
            session["rol"] = user.get("rol", "usuario")

            flash("Inicio de sesión correcto.", "success")
            return redirect(url_for("panel_usuarios"))
        else:
            flash("Usuario o contraseña incorrectos.", "danger")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada.", "info")
    return redirect(url_for("index"))


# ---------------------- Panel administrador / usuarios ----------------------
@app.route("/panel-usuarios")
def panel_usuarios():
    """
    Panel de administración de usuarios.
    Solo accesible para usuarios con rol 'admin'.
    """
    if "user_id" not in session:
        flash("Debes iniciar sesión para acceder al panel.", "warning")
        return redirect(url_for("login"))

    if session.get("rol") != "admin":
        flash("Debes ser un usuario admin para ver usuarios registrados.", "danger")
        return redirect(url_for("login"))

    if not usuarios_col:
        flash("Base de datos de usuarios no disponible.", "danger")
        return redirect(url_for("index"))

    usuarios = list(
        usuarios_col.find(
            {},
            {
                "username": 1,
                "email": 1,
                "rol": 1,
                "activo": 1,
                "ultimo_acceso": 1,
            },
        )
    )

    return render_template("admin.html", usuarios=usuarios)


# ---------------------- Buscador (Elasticsearch) ----------------------
@app.route("/buscador")
def buscador():
    """
    Buscador sobre Elasticsearch.
    """
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

            hits = resp.get("hits", {}).get("hits", [])
            for hit in hits:
                source = hit.get("_source", {}) or {}

                # Intentamos con MUCHOS posibles nombres de campos
                titulo = _first_existing(
                    source,
                    [
                        "titulo",
                        "Titulo",
                        "Título",
                        "TITULO",
                        "titulo_norma",
                        "Titulo_norma",
                        "Título_norma",
                        "TITULO_NORMA",
                        "nombre_norma",
                        "Nombre_norma",
                    ],
                    default=None,
                )

                entidad = _first_existing(
                    source,
                    [
                        "entidad",
                        "Entidad",
                        "ENTIDAD",
                        "entidad_emisora",
                        "Entidad_emisora",
                        "ENTIDAD_EMISORA",
                        "organismo",
                        "Organismo",
                    ],
                    default=None,
                )

                anio = _first_existing(
                    source,
                    [
                        "anio",
                        "Anio",
                        "Año",
                        "ANO",
                        "anio_publicacion",
                        "Anio_publicacion",
                        "Año_publicacion",
                        "ano_publicacion",
                    ],
                    default=None,
                )

                tipo = _first_existing(
                    source,
                    [
                        "tipo",
                        "Tipo",
                        "TIPO",
                        "tipo_norma",
                        "Tipo_norma",
                        "TIPO_NORMA",
                        "clase_norma",
                        "Clase_norma",
                    ],
                    default=None,
                )

                resultados.append(
                    {
                        "id": hit.get("_id"),
                        "score": hit.get("_score"),
                        "titulo": titulo,
                        "entidad": entidad,
                        "anio": anio,
                        "tipo": tipo,
                    }
                )

        except Exception as e:
            error_msg = f"Error al consultar Elasticsearch: {e}"

    elif q and not elastic_configured_flag:
        error_msg = "El buscador no está configurado (Elasticsearch sin credenciales)."

    return render_template(
        "buscador.html",
        query=q,
        resultados=resultados,
        elastic_configured=elastic_configured_flag,
        error_msg=error_msg,
    )


# =====================================================================
#                         Punto de entrada local
# =====================================================================

if __name__ == "__main__":
    app.run(debug=True)
