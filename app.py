import os
from datetime import datetime
from functools import wraps

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

# ---------------------- MongoDB (usuarios) ----------------------
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

# Soporte alterno por URL simple (opcional)
ELASTIC_URL = os.getenv("ELASTIC_URL")

ELASTIC_INDEX_DEFAULT = os.getenv(
    "ELASTIC_INDEX_DEFAULT",
    os.getenv("ELASTIC_INDEX", "minminas-normatividad"),
)

elastic_client = None
elastic_configured = False

try:
    if ELASTIC_CLOUD_ID and ELASTIC_API_KEY:
        elastic_client = Elasticsearch(
            cloud_id=ELASTIC_CLOUD_ID,
            api_key=ELASTIC_API_KEY,
        )
    elif ELASTIC_URL:
        elastic_client = Elasticsearch(ELASTIC_URL)

    if elastic_client is not None:
        info = elastic_client.info()
        print(f"[OK] Elasticsearch conectado: {info['cluster_name']}")
        elastic_configured = True
    else:
        print("[WARN] No hay configuración válida para Elasticsearch.")

except Exception as e:
    print(f"[ERROR] Conectando a Elasticsearch: {e}")

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


def _extract_year(source: dict):
    """
    Intenta obtener el año desde diferentes campos (anio, año, year, fecha...).
    """
    year = _first_existing(
        source,
        [
            "anio",
            "Anio",
            "Año",
            "ANO",
            "ano",
            "year",
            "Year",
            "YEAR",
            "anio_publicacion",
            "Anio_publicacion",
            "Año_publicacion",
            "ano_publicacion",
        ],
        default=None,
    )

    if year not in (None, ""):
        return year

    # Intentar derivar el año desde una fecha completa
    fecha = _first_existing(
        source,
        [
            "fecha",
            "Fecha",
            "fecha_publicacion",
            "Fecha_publicacion",
            "FECHA",
            "date",
            "Date",
        ],
        default=None,
    )

    if fecha:
        s = str(fecha)
        candidatos = [s[:4], s[-4:]]
        for c in candidatos:
            if c.isdigit():
                return c

    return "N/D"


def normalizar_resultados_es(resp):
    """
    Convierte la respuesta de Elasticsearch en una lista de dicts
    con las claves: titulo, entidad, anio, tipo, score.
    """
    resultados = []

    hits = resp.get("hits", {}).get("hits", [])

    for hit in hits:
        src = hit.get("_source", {}) or {}

        titulo = _first_existing(
            src,
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
                "title",
                "Title",
                "name",
                "Name",
            ],
            default="Sin título",
        )

        entidad = _first_existing(
            src,
            [
                "entidad",
                "Entidad",
                "ENTIDAD",
                "entidad_emisora",
                "Entidad_emisora",
                "ENTIDAD_EMISORA",
                "organismo",
                "Organismo",
                "emisor",
                "Emisor",
                "issuer",
                "Issuer",
                "entity",
                "Entity",
            ],
            default="N/D",
        )

        anio = _extract_year(src)

        tipo = _first_existing(
            src,
            [
                "tipo",
                "Tipo",
                "TIPO",
                "tipo_norma",
                "Tipo_norma",
                "TIPO_NORMA",
                "clase_norma",
                "Clase_norma",
                "document_type",
                "Document_type",
                "tipo_documento",
                "Tipo_documento",
            ],
            default="N/D",
        )

        resultados.append(
            {
                "id": hit.get("_id"),
                "score": float(hit.get("_score", 0.0) or 0.0),
                "titulo": titulo,
                "entidad": entidad,
                "anio": anio,
                "tipo": tipo,
            }
        )

    total_raw = resp.get("hits", {}).get("total", 0)
    if isinstance(total_raw, dict):
        total = total_raw.get("value", 0)
    else:
        total = total_raw

    return resultados, total


def buscar_normas(query, size=20):
    """
    Ejecuta el query en Elasticsearch y devuelve
    (resultados_normalizados, total_documentos, error_msg)
    """
    if not query:
        return [], 0, None

    if not current_app.config.get("ELASTIC_CONFIGURED", False):
        return [], 0, "El buscador no está configurado (Elasticsearch sin credenciales)."

    es = current_app.config["ELASTIC_CLIENT"]
    index_name = current_app.config["ELASTIC_INDEX_DEFAULT"]

    try:
        resp = es.search(
            index=index_name,
            query={
                "multi_match": {
                    "query": query,
                    "fields": [
                        "titulo^3",
                        "Titulo^3",
                        "Título^3",
                        "titulo_norma^3",
                        "texto",
                        "texto_completo",
                        "entidad",
                        "entidad_emisora",
                        "tipo",
                        "clase_norma",
                    ],
                    "type": "best_fields",
                }
            },
            size=size,
        )

        resultados, total = normalizar_resultados_es(resp)
        return resultados, total, None

    except Exception as e:
        return [], 0, f"Error al consultar Elasticsearch: {e}"


# ---------------------- Utilidades de sesión / roles ----------------------

def usuario_actual():
    if "user_id" not in session:
        return None
    return {
        "id": session["user_id"],
        "username": session.get("username"),
        "rol": session.get("rol", "usuario"),
    }


def es_admin_actual():
    u = usuario_actual()
    return bool(u) and u["rol"] == "admin"


def requiere_admin(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not es_admin_actual():
            flash("Debes ingresar como administrador para ver esta opción.", "warning")
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return wrapper


@app.context_processor
def inject_current_user():
    """
    Inyecta current_user en todas las plantillas.
    """
    return {"current_user": usuario_actual()}


# =====================================================================
#                               RUTAS
# =====================================================================

# ---------------------- Landing / inicio ----------------------
@app.route("/")
def index():
    # landing principal (ya tienes templates/landing.html o index.html)
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
      - Redirige al buscador o al panel admin según rol.
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
            session["username"] = user.get("username") or user.get("email")
            session["rol"] = user.get("rol", "usuario")

            flash("Inicio de sesión correcto.", "success")

            if session["rol"] == "admin":
                return redirect(url_for("admin_usuarios"))
            else:
                return redirect(url_for("buscador"))
        else:
            flash("Usuario o contraseña incorrectos.", "danger")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada.", "info")
    return redirect(url_for("index"))


# ---------------------- Buscador (Elasticsearch) ----------------------
@app.route("/buscador", methods=["GET"])
def buscador():
    """
    Buscador sobre Elasticsearch utilizando las funciones de normalización.
    """
    q = request.args.get("q", "").strip()
    resultados = []
    total = 0
    error_msg = None

    if q:
        resultados, total, error_msg = buscar_normas(q)

    return render_template(
        "buscador.html",
        query=q,
        resultados=resultados,
        total=total,
        elastic_configured=current_app.config.get("ELASTIC_CONFIGURED", False),
        error_msg=error_msg,
    )


# ---------------------- Administración de usuarios ----------------------
# Alias antiguo para no romper enlaces viejos
@app.route("/panel-usuarios")
def panel_usuarios():
    return redirect(url_for("admin_usuarios"))


@app.route("/admin/usuarios", methods=["GET"])
@requiere_admin
def admin_usuarios():
    """
    Listado de usuarios (solo admin).
    """
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
        ).sort("username", 1)
    )

    return render_template("admin_usuarios.html", usuarios=usuarios)


@app.route("/admin/usuarios/nuevo", methods=["GET", "POST"])
@requiere_admin
def admin_crear_usuario():
    """
    Crea un nuevo usuario y le asigna rol.
    """
    if not usuarios_col:
        flash("Base de datos de usuarios no disponible.", "danger")
        return redirect(url_for("index"))

    error_msg = None

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        email = (request.form.get("email") or "").strip()
        rol = request.form.get("rol") or "usuario"
        password = request.form.get("password") or ""

        if not username or not email or not password:
            error_msg = "Todos los campos son obligatorios."
        elif usuarios_col.find_one(
            {"$or": [{"username": username}, {"email": email}]}
        ):
            error_msg = "Ya existe un usuario con ese nombre de usuario o correo."
        else:
            usuarios_col.insert_one(
                {
                    "username": username,
                    "email": email,
                    "rol": rol,
                    "password": generate_password_hash(password),
                    "activo": True,
                    "creado_en": datetime.utcnow(),
                }
            )
            flash("Usuario creado correctamente.", "success")
            return redirect(url_for("admin_usuarios"))

    return render_template("admin_crear_usuario.html", error_msg=error_msg)


# =====================================================================
#                         Punto de entrada local
# =====================================================================

if __name__ == "__main__":
    app.run(debug=True)
