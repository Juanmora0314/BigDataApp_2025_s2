import os
from datetime import datetime

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
    jsonify,
)
from pymongo import MongoClient
from bson.objectid import ObjectId
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv

try:
    # Cliente oficial de Elasticsearch 9.x
    from elasticsearch import Elasticsearch
except ImportError:
    Elasticsearch = None  # Para evitar errores si falta la librería en local


# ===================== Configuración base =====================

load_dotenv()

app = Flask(__name__)

# SECRET_KEY para sesiones
app.secret_key = os.getenv("SECRET_KEY", "dev_key_cambia_esto")

# ------------------ MongoDB (usuarios) ------------------

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "minminas_app")
MONGO_COLECCION = os.getenv("MONGO_COLECCION", "usuarios")

mongo_client = None
usuarios_col = None

if MONGO_URI:
    try:
        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DB]
        usuarios_col = mongo_db[MONGO_COLECCION]
    except Exception as e:
        print(f"[MongoDB] Error conectando a Mongo: {e}")


# ------------------ Elasticsearch (normatividad) ------------------

ELASTIC_CLOUD_ID = os.getenv("ELASTIC_CLOUD_ID")
ELASTIC_CLOUD_URL = os.getenv("ELASTIC_CLOUD_URL")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")
ELASTIC_INDEX_DEFAULT = os.getenv("ELASTIC_INDEX_DEFAULT", "minminas-normatividad")

es = None
if Elasticsearch is not None and ELASTIC_API_KEY:
    try:
        if ELASTIC_CLOUD_ID:
            es = Elasticsearch(cloud_id=ELASTIC_CLOUD_ID, api_key=ELASTIC_API_KEY)
        elif ELASTIC_CLOUD_URL:
            es = Elasticsearch(ELASTIC_CLOUD_URL, api_key=ELASTIC_API_KEY)
    except Exception as e:
        print(f"[Elasticsearch] Error conectando al cluster: {e}")


# ===================== Helpers =====================

def usuario_actual():
    """Devuelve el documento del usuario logueado o None."""
    if not usuarios_col:
        return None
    user_id = session.get("user_id")
    if not user_id:
        return None
    try:
        return usuarios_col.find_one({"_id": ObjectId(user_id)})
    except Exception:
        return None


def login_required(func):
    """Decorador simple para exigir login."""
    from functools import wraps

    @wraps(func)
    def wrapper(*args, **kwargs):
        if not usuario_actual():
            flash("Debes iniciar sesión para acceder a esta página.", "warning")
            return redirect(url_for("login"))
        return func(*args, **kwargs)

    return wrapper


# ===================== Rutas generales =====================

@app.route("/healthz")
def healthz():
    """Ruta de health check (puede usarse en Render si quieres)."""
    return jsonify(status="ok", time=datetime.utcnow().isoformat())


@app.route("/")
def landing():
    """Página de inicio / landing."""
    # Asegúrate de tener templates/landing.html
    return render_template("landing.html")


@app.route("/about")
def about():
    """Página about básica."""
    # templates/about.html
    return render_template("about.html")


# ===================== Autenticación =====================

@app.route("/login", methods=["GET", "POST"])
def login():
    """
    Login de usuarios.
    Formulario esperado en login.html:
      - input name="email"
      - input name="password"
    """
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        if not usuarios_col:
            flash("No hay conexión con la base de usuarios.", "danger")
            return redirect(url_for("login"))

        user = usuarios_col.find_one({"email": email})
        if not user or not check_password_hash(user["password_hash"], password):
            flash("Usuario o contraseña incorrectos.", "danger")
            return redirect(url_for("login"))

        # Login OK
        session["user_id"] = str(user["_id"])
        session["user_name"] = user.get("nombre", email)
        flash(f"Bienvenido, {session['user_name']} 👋", "success")
        return redirect(url_for("admin"))

    return render_template("login.html")


@app.route("/logout")
def logout():
    """Cierra sesión y vuelve al landing."""
    session.clear()
    flash("Sesión cerrada correctamente.", "info")
    return redirect(url_for("landing"))


@app.route("/registro", methods=["GET", "POST"])
def registro():
    """
    Registro de usuarios.
    Formulario esperado en algún template (p.ej. admin.html o registro.html):
      - input name="nombre"
      - input name="email"
      - input name="password"
    """
    if request.method == "POST":
        nombre = request.form.get("nombre", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        if not usuarios_col:
            flash("No hay conexión con la base de usuarios.", "danger")
            return redirect(url_for("registro"))

        if not nombre or not email or not password:
            flash("Todos los campos son obligatorios.", "warning")
            return redirect(url_for("registro"))

        existente = usuarios_col.find_one({"email": email})
        if existente:
            flash("Ya existe un usuario con ese correo.", "warning")
            return redirect(url_for("registro"))

        password_hash = generate_password_hash(password)

        usuarios_col.insert_one(
            {
                "nombre": nombre,
                "email": email,
                "password_hash": password_hash,
                "rol": "user",
                "creado_en": datetime.utcnow(),
            }
        )

        flash("Usuario registrado correctamente. Ahora puedes iniciar sesión.", "success")
        return redirect(url_for("login"))

    # Si quieres puedes hacer un template aparte, o reutilizar admin/otro
    return render_template("admin.html", modo="registro")


# ===================== Panel de administración =====================

@app.route("/admin")
@login_required
def admin():
    """
    Panel básico de administración de usuarios.
    Renderiza templates/admin.html con la lista de usuarios.
    """
    if not usuarios_col:
        flash("No hay conexión con la base de usuarios.", "danger")
        usuarios = []
    else:
        usuarios = list(
            usuarios_col.find().sort("creado_en", -1)
        )

    return render_template(
        "admin.html",
        usuario=usuario_actual(),
        usuarios=usuarios,
    )


# ===================== Búsqueda en Elasticsearch =====================

@app.route("/buscador", methods=["GET", "POST"])
def buscador():
    """
    Buscador de normatividad.
    Usa Elasticsearch para consultar el índice por defecto.
    Formulario esperado (p.ej. en buscador.html):
      - input name="q"
    """
    consulta = request.args.get("q") or request.form.get("q")
    resultados = []
    total = 0

    if consulta and es:
        try:
            resp = es.search(
                index=ELASTIC_INDEX_DEFAULT,
                size=25,
                query={
                    "multi_match": {
                        "query": consulta,
                        "fields": [
                            "nombre_archivo^3",
                            "titulo^2",
                            "descripcion",
                            "contenido",
                        ],
                    }
                },
            )
            hits = resp.get("hits", {}).get("hits", [])
            total = resp.get("hits", {}).get("total", {}).get("value", len(hits))

            for h in hits:
                src = h.get("_source", {})
                resultados.append(
                    {
                        "id": h.get("_id"),
                        "score": h.get("_score"),
                        "nombre_archivo": src.get("nombre_archivo"),
                        "titulo": src.get("titulo"),
                        "descripcion": src.get("descripcion"),
                        "fecha": src.get("fecha"),
                        "url": src.get("url"),
                    }
                )
        except Exception as e:
            print(f"[Elasticsearch] Error en búsqueda: {e}")
            flash("Ocurrió un error al buscar en Elasticsearch.", "danger")

    return render_template(
        "buscador.html",
        query=consulta,
        resultados=resultados,
        total=total,
    )


@app.route("/documentos")
def documentos():
    """
    Lista sencilla de documentos (match_all) para templates/documentos_elastic.html
    """
    documentos = []
    if es:
        try:
            resp = es.search(
                index=ELASTIC_INDEX_DEFAULT,
                size=50,
                query={"match_all": {}},
                sort=[{"fecha": {"order": "desc"}}],
            )
            for h in resp.get("hits", {}).get("hits", []):
                src = h.get("_source", {})
                documentos.append(
                    {
                        "id": h.get("_id"),
                        "nombre_archivo": src.get("nombre_archivo"),
                        "titulo": src.get("titulo"),
                        "descripcion": src.get("descripcion"),
                        "fecha": src.get("fecha"),
                        "url": src.get("url"),
                    }
                )
        except Exception as e:
            print(f"[Elasticsearch] Error listando documentos: {e}")
            flash("No se pudieron cargar los documentos desde Elasticsearch.", "danger")

    return render_template("documentos_elastic.html", documentos=documentos)


# ===================== Punto de entrada local =====================

if __name__ == "__main__":
    # Para desarrollo local. En Render se ignora y se usa gunicorn app:app
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
