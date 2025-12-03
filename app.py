import os
from functools import wraps

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
from passlib.hash import bcrypt
from elasticsearch import Elasticsearch

# ---------------------- Config básica ---------------------- #

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")

# MongoDB
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/minminas")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["minminas"]
usuarios_col = db["usuarios"]

# Elasticsearch
ES_URL = os.environ.get("ES_URL")  # ej: "https://xxx.es.io:443"
ES_USER = os.environ.get("ES_USER")
ES_PASSWORD = os.environ.get("ES_PASSWORD")
ES_INDEX = os.environ.get("ES_INDEX", "normas_minminas")

if ES_URL:
    es = Elasticsearch(
        ES_URL,
        basic_auth=(ES_USER, ES_PASSWORD) if ES_USER else None,
    )
else:
    es = None


# ---------------------- Helpers ---------------------- #

@app.context_processor
def inject_user():
    """Hace que current_user esté disponible en todos los templates."""
    return {"current_user": session.get("user")}


def requiere_admin(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        user = session.get("user")
        if not user or user.get("rol") != "admin":
            flash(
                "Debes iniciar sesión como administrador para ver esta sección.",
                "warning",
            )
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


# ---------------------- Rutas públicas ---------------------- #

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/buscador")
def buscador():
    q = request.args.get("q", "").strip()
    results = []
    total = 0
    error = None

    if q:
        if es is None:
            error = "El buscador no está configurado (Elasticsearch sin URL)."
        else:
            try:
                resp = es.search(
                    index=ES_INDEX,
                    query={
                        "multi_match": {
                            "query": q,
                            "fields": [
                                "titulo^3",
                                "resumen",
                                "descripcion",
                                "texto",
                            ],
                        }
                    },
                    size=20,
                )
                total = resp["hits"]["total"]["value"]
                for hit in resp["hits"]["hits"]:
                    s = hit["_source"]
                    results.append(
                        {
                            "titulo": s.get("titulo") or "Documento sin título",
                            "resumen": s.get("resumen") or s.get("descripcion"),
                            "fecha": s.get("fecha"),
                            "entidad": s.get("entidad"),
                            "enlace": s.get("enlace") or s.get("url"),
                        }
                    )
            except Exception as exc:  # noqa: F841
                app.logger.exception("Error consultando Elasticsearch")
                # OJO: ya no soltamos 500, devolvemos página normal con mensaje
                error = (
                    "Error al comunicarse con el servidor de búsqueda. "
                    "Inténtalo de nuevo más tarde."
                )

    return render_template(
        "buscador.html",
        q=q,
        results=results,
        total=total,
        error=error,
    )


# ---------------------- Autenticación ---------------------- #

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username_or_email = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")

        user = usuarios_col.find_one(
            {
                "$or": [
                    {"username": username_or_email},
                    {"email": username_or_email},
                ]
            }
        )

        stored_hash = None
        if user:
            # Compatibilidad: password_hash o password
            stored_hash = user.get("password_hash") or user.get("password")

        if not user or not stored_hash or not bcrypt.verify(password, stored_hash):
            flash("Usuario o contraseña incorrectos", "danger")
            return redirect(url_for("login"))

        session["user"] = {
            "id": str(user["_id"]),
            "username": user.get("username"),
            "email": user.get("email"),
            "rol": user.get("rol", "usuario"),
        }
        flash(f"Bienvenido, {user.get('username')}!", "success")
        return redirect(url_for("panel_usuarios"))

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.pop("user", None)
    flash("Sesión cerrada correctamente.", "info")
    return redirect(url_for("index"))


# ---------------------- Panel de usuarios ---------------------- #

@app.route("/usuarios")
@requiere_admin
def panel_usuarios():
    usuarios = list(usuarios_col.find().sort("username"))
    return render_template("usuarios.html", usuarios=usuarios)


# ---------------------- Main ---------------------- #

if __name__ == "__main__":
    app.run(debug=True)
