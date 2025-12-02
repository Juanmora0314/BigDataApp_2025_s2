# app.py
# ============================================================
# Proyecto BigData 2025-S2 – MinMinas Normatividad
# Front Flask + MongoDB (usuarios) + Elasticsearch (normas)
# ============================================================

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    jsonify,
    session,
    flash,
)
from dotenv import load_dotenv
from werkzeug.utils import secure_filename
from elasticsearch import Elasticsearch

import os
from datetime import datetime
from typing import Dict, Any, List

# Helpers propios
from Helpers import ElasticSearch, Funciones, WebScraping, MongoDBUsuarios

# ------------------------------------------------------------
# Carga de variables de entorno
# ------------------------------------------------------------
load_dotenv()

app = Flask(
    __name__,
    static_folder="static",       # aquí vive tu CSS (static/css/...)
    static_url_path="/static",    # URL pública para /static/...
    template_folder="templates",  # aquí viven tus .html
)
app.secret_key = os.getenv("SECRET_KEY", "clave_super_secreta_dev")

# ==========================================
#   CONFIGURACIÓN DE ELASTICSEARCH (buscador público)
# ==========================================
ELASTIC_URL = os.getenv("ELASTIC_URL", "http://localhost:9200")
ELASTIC_USER = os.getenv("ELASTIC_USER")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")
ELASTIC_INDEX = os.getenv("ELASTIC_INDEX", "normas")

if ELASTIC_USER and ELASTIC_PASSWORD:
    es = Elasticsearch(
        ELASTIC_URL,
        basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        verify_certs=False,  # pon True si tienes certificados OK
    )
else:
    # Solo para clusters sin autenticación
    es = Elasticsearch(ELASTIC_URL, verify_certs=False)

# --- Mongo (gestión de usuarios) ---
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "minminas_app")
MONGO_COLECCION_USUARIOS = os.getenv("MONGO_COLECCION", "usuarios")

# --- Elasticsearch (normatividad MinMinas – administración) ---
ELASTIC_CLOUD_ID = os.getenv("ELASTIC_CLOUD_ID")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")
ELASTIC_INDEX_MINMINAS = os.getenv("ELASTIC_INDEX_MINMINAS", "minminas-normatividad")

# Metadatos de la app
VERSION_APP = "1.0.0"
CREATOR_APP = "Juan Sebastián Mora Zapata"

# Validaciones mínimas de configuración
if not MONGO_URI:
    raise RuntimeError("Falta la variable de entorno MONGO_URI en el .env")

if not ELASTIC_CLOUD_ID or not ELASTIC_API_KEY:
    raise RuntimeError(
        "Faltan ELASTIC_CLOUD_ID o ELASTIC_API_KEY en el .env para conectarse a Elasticsearch"
    )

# ------------------------------------------------------------
# Inicializar clientes de Mongo y Elastic (administración)
# ------------------------------------------------------------
mongo_users = MongoDBUsuarios(
    uri=MONGO_URI,
    db_name=MONGO_DB,
    coleccion=MONGO_COLECCION_USUARIOS,
)

elastic = ElasticSearch(
    cloud_id=ELASTIC_CLOUD_ID,
    api_key=ELASTIC_API_KEY,
    default_index=ELASTIC_INDEX_MINMINAS,
)

# Carpeta para uploads temporales (ZIP, PDFs, JSONs)
UPLOAD_FOLDER = os.path.join("static", "uploads")
Funciones.crear_carpeta(UPLOAD_FOLDER)


# ------------------------------------------------------------
# Helpers de permisos/sesión
# ------------------------------------------------------------
def build_permisos(user_doc: Dict[str, Any]) -> Dict[str, bool]:
    """
    A partir del rol del usuario construye el diccionario de permisos
    que usaremos en la sesión y en las plantillas.
    """
    rol = user_doc.get("rol", "analista")

    return {
        "login": True,
        "admin_usuarios": rol == "admin",
        "admin_elastic": rol in ("admin", "analista"),
        "admin_data_elastic": rol == "admin",
    }


def login_requerido() -> bool:
    """Devuelve True si NO hay sesión activa (para usar en if rápidos)."""
    return not session.get("logged_in")


def require_login_y_permiso(nombre_permiso: str):
    """
    Helper para rutas protegidas: valida login y un permiso concreto.
    Devuelve (bool, respuesta_flask_opcional). Si el bool es False,
    hay que devolver la respuesta.
    """
    if not session.get("logged_in"):
        flash("Por favor, inicia sesión para acceder a esta página", "warning")
        return False, redirect(url_for("login"))

    permisos = session.get("permisos", {})
    if not permisos.get(nombre_permiso, False):
        flash("No tienes permisos para acceder a esta sección", "danger")
        return False, redirect(url_for("admin"))

    return True, None


# ============================================================
# RUTAS PÚBLICAS BÁSICAS
# ============================================================

@app.route("/")
@app.route("/index")
@app.route("/home")
def landing():
    """Landing page pública."""
    return render_template(
        "landing.html",
        version=VERSION_APP,
        creador=CREATOR_APP,
    )


@app.route("/about")
def about():
    """Página About."""
    return render_template(
        "about.html",
        version=VERSION_APP,
        creador=CREATOR_APP,
    )


# ==========================================
#   VISTAS DEL BUSCADOR PÚBLICO
# ==========================================

@app.route("/buscador")
def buscador():
    """Página del buscador público sobre Elasticsearch."""
    return render_template(
        "buscador.html",
        version=VERSION_APP,
        creador=CREATOR_APP,
    )


@app.route("/buscar-elastic", methods=["POST"])
def buscar_elastic():
    """
    API pública para realizar búsqueda de texto en Elasticsearch
    sobre el índice configurado en ELASTIC_INDEX.
    Espera JSON: {"texto": "...", "size": 10 opcional}
    """
    data = request.get_json(silent=True) or {}
    texto = (data.get("texto") or "").strip()
    size = int(data.get("size") or 20)

    if not texto:
        return jsonify({"success": False, "error": "Texto de búsqueda vacío."}), 400

    try:
        # Búsqueda básica en Elasticsearch
        resp = es.search(
            index=ELASTIC_INDEX,
            query={
                "multi_match": {
                    "query": texto,
                    "fields": [
                        "titulo",
                        "texto",
                        "resumen",
                        "tipo_norma",
                        "entidad",
                        "numero_norma",
                    ],
                }
            },
            size=size,
        )

        # Devolvemos la respuesta tal cual; el JS ya sabe leer hits / hits.total
        return jsonify(resp)

    except Exception as e:
        # Aquí es donde veías AuthenticationException(401, ...)
        print("Error en búsqueda Elastic:", e)
        return jsonify({"success": False, "error": str(e)}), 500


# ============================================================
# AUTENTICACIÓN (MongoDB usuarios)
# ============================================================

@app.route("/login", methods=["GET", "POST"])
@app.route("/ingresar", methods=["GET", "POST"])  # alias para el botón "Ingresar"
def login():
    """Página de login + validación en MongoDB Atlas."""
    if request.method == "POST":
        usuario = (request.form.get("usuario") or "").strip()
        password = request.form.get("password") or ""

        # Permite login por username o email
        user_data = mongo_users.validar_usuario(usuario, password)

        if user_data:
            permisos = build_permisos(user_data)

            session["logged_in"] = True
            session["usuario"] = user_data.get("username")
            session["user_id"] = user_data.get("id")
            session["rol"] = user_data.get("rol", "analista")
            session["permisos"] = permisos

            flash("¡Bienvenido! Inicio de sesión exitoso", "success")
            return redirect(url_for("admin"))
        else:
            flash("Usuario o contraseña incorrectos", "danger")

    return render_template(
        "login.html",
        version=VERSION_APP,
        creador=CREATOR_APP,
    )


@app.route("/logout")
def logout():
    """Cerrar sesión."""
    session.clear()
    flash("Sesión cerrada correctamente", "info")
    return redirect(url_for("landing"))


# API para listar usuarios (se usa en login para ver quiénes existen)
@app.route("/listar-usuarios-api")
@app.route("/listar-usuarios")  # alias compatible con código anterior
def listar_usuarios_api():
    """
    Devuelve un JSON con los usuarios almacenados en MongoDB.
    Solo campos básicos, sin password_hash.
    """
    try:
        usuarios_raw = mongo_users.listar_usuarios(solo_activos=False, limit=100)

        usuarios: List[Dict[str, Any]] = []
        for u in usuarios_raw:
            u2 = {
                "id": u.get("id"),
                "username": u.get("username"),
                "email": u.get("email"),
                "rol": u.get("rol", "analista"),
                "activo": bool(u.get("activo", True)),
            }
            for campo in ("created_at", "updated_at", "ultimo_login"):
                val = u.get(campo)
                if isinstance(val, datetime):
                    u2[campo] = val.isoformat()
            usuarios.append(u2)

        return jsonify(usuarios)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================================
# ADMINISTRACIÓN GENERAL
# ============================================================

@app.route("/admin")
def admin():
    """Panel simple de administración (requiere login)."""
    if login_requerido():
        flash(
            "Por favor, inicia sesión para acceder al área de administración",
            "warning",
        )
        return redirect(url_for("login"))

    return render_template(
        "admin.html",
        usuario=session.get("usuario"),
        permisos=session.get("permisos", {}),
        version=VERSION_APP,
        creador=CREATOR_APP,
    )


# ============================================================
# GESTOR DE USUARIOS (Mongo) – vistas básicas
# ============================================================

@app.route("/gestor_usuarios")
def gestor_usuarios():
    """
    Página de gestión de usuarios.
    Requiere login + permiso admin_usuarios.
    """
    ok, resp = require_login_y_permiso("admin_usuarios")
    if not ok:
        return resp

    return render_template(
        "gestor_usuarios.html",
        usuario=session.get("usuario"),
        permisos=session.get("permisos", {}),
        version=VERSION_APP,
        creador=CREATOR_APP,
    )


@app.route("/crear-usuario", methods=["POST"])
def crear_usuario():
    """API para crear un nuevo usuario (admin_usuarios)."""
    ok, resp = require_login_y_permiso("admin_usuarios")
    if not ok:
        return jsonify({"success": False, "error": "No autorizado"}), resp.status_code

    try:
        data = request.get_json(silent=True) or {}

        username = (data.get("usuario") or data.get("username") or "").strip()
        email = (data.get("email") or "").strip()
        password = data.get("password") or ""
        rol = data.get("rol", "analista")
        activo = bool(data.get("activo", True))

        if not username or not password:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "Usuario y contraseña son obligatorios",
                    }
                ),
                400,
            )

        # Si no mandan email, inventamos uno básico para cumplir la unicidad
        if not email:
            email = f"{username}@example.local"

        new_id = mongo_users.crear_usuario(
            username=username,
            email=email,
            password=password,
            rol=rol,
            activo=activo,
        )

        if not new_id:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "No se pudo crear el usuario (¿duplicado?)",
                    }
                ),
                500,
            )

        return jsonify({"success": True, "id": new_id})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/eliminar-usuario", methods=["POST"])
def eliminar_usuario():
    """API para eliminar un usuario (admin_usuarios)."""
    ok, resp = require_login_y_permiso("admin_usuarios")
    if not ok:
        return jsonify({"success": False, "error": "No autorizado"}), resp.status_code

    try:
        data = request.get_json(silent=True) or {}
        user_id = data.get("id") or data.get("user_id")

        if not user_id:
            return (
                jsonify(
                    {"success": False, "error": "Debe enviar el id del usuario"}
                ),
                400,
            )

        # Evitar que un usuario se borre a sí mismo
        if user_id == session.get("user_id"):
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "No puedes eliminar tu propio usuario en esta vista",
                    }
                ),
                400,
            )

        ok_delete = mongo_users.eliminar_usuario(user_id)
        return jsonify({"success": ok_delete})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/actualizar-usuario", methods=["POST"])
def actualizar_usuario():
    """API para actualizar datos de un usuario (admin_usuarios)."""
    ok, resp = require_login_y_permiso("admin_usuarios")
    if not ok:
        return jsonify({"success": False, "error": "No autorizado"}), resp.status_code

    try:
        data = request.get_json(silent=True) or {}
        user_id = data.get("id") or data.get("user_id")
        nuevos_datos = data.get("datos", {})

        if not user_id:
            return (
                jsonify(
                    {"success": False, "error": "Debe enviar el id del usuario"}
                ),
                400,
            )

        # No permitimos cambiar password por aquí (hay método específico)
        nuevos_datos.pop("password", None)
        nuevos_datos.pop("password_hash", None)

        ok_update = mongo_users.actualizar_usuario(user_id, nuevos_datos)
        return jsonify({"success": ok_update})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ============================================================
# GESTOR ELASTICSEARCH (índices y carga de datos)
# ============================================================

@app.route("/gestor_elastic")
def gestor_elastic():
    """Página para administración de índices Elastic (admin_elastic)."""
    ok, resp = require_login_y_permiso("admin_elastic")
    if not ok:
        return resp

    return render_template(
        "gestor_elastic.html",
        usuario=session.get("usuario"),
        permisos=session.get("permisos", {}),
        version=VERSION_APP,
        creador=CREATOR_APP,
    )


@app.route("/listar-indices-elastic")
def listar_indices_elastic():
    """API para listar índices de Elasticsearch (admin_elastic)."""
    ok, resp = require_login_y_permiso("admin_elastic")
    if not ok:
        return jsonify({"error": "No autorizado"}), resp.status_code

    try:
        indices = elastic.listar_indices()
        return jsonify(indices)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/ejecutar-query-elastic", methods=["POST"])
def ejecutar_query_elastic():
    """
    API para ejecutar comandos de administración en Elasticsearch.
    Usa el método ejecutar_comando de la clase ElasticSearch.
    """
    ok, resp = require_login_y_permiso("admin_elastic")
    if not ok:
        return jsonify({"success": False, "error": "No autorizado"}), resp.status_code

    try:
        data = request.get_json(silent=True) or {}
        query_json = data.get("query")

        if not query_json:
            return (
                jsonify(
                    {"success": False, "error": "El JSON de comando es requerido"}
                ),
                400,
            )

        resultado = elastic.ejecutar_comando(query_json)
        return jsonify(resultado)

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/cargar_doc_elastic")
def cargar_doc_elastic():
    """
    Vista para cargar documentos (ZIP/JSON/PDF) a Elasticsearch.
    Requiere permiso admin_data_elastic.
    """
    ok, resp = require_login_y_permiso("admin_data_elastic")
    if not ok:
        return resp

    return render_template(
        "documentos_elastic.html",
        usuario=session.get("usuario"),
        permisos=session.get("permisos", {}),
        version=VERSION_APP,
        creador=CREATOR_APP,
    )


# -------------------- WebScraping → archivos locales --------------------

@app.route("/procesar-webscraping-elastic", methods=["POST"])
def procesar_webscraping_elastic():
    """
    Ejecuta web scraping dado una URL inicial, descarga PDFs y
    lista los archivos encontrados. No indexa todavía en Elastic.
    """
    ok, resp = require_login_y_permiso("admin_data_elastic")
    if not ok:
        return jsonify({"success": False, "error": "No autorizado"}), resp.status_code

    try:
        data = request.get_json(silent=True) or {}

        url = data.get("url")
        extensiones_navegar = data.get("extensiones_navegar", "aspx")
        tipos_archivos = data.get("tipos_archivos", "pdf")

        if not url:
            return (
                jsonify({"success": False, "error": "La URL es requerida"}),
                400,
            )

        lista_ext_navegar = [ext.strip() for ext in extensiones_navegar.split(",")]
        lista_tipos_archivos = [ext.strip() for ext in tipos_archivos.split(",")]

        todas_extensiones = lista_ext_navegar + lista_tipos_archivos

        # Dominio base a partir de la URL
        dominio_base = url.rsplit("/", 1)[0] + "/"
        scraper = WebScraping(dominio_base=dominio_base)

        Funciones.crear_carpeta(UPLOAD_FOLDER)
        Funciones.borrar_contenido_carpeta(UPLOAD_FOLDER)

        json_path = os.path.join(UPLOAD_FOLDER, "links.json")

        resultado_links = scraper.extraer_todos_los_links(
            url_inicial=url,
            json_file_path=json_path,
            listado_extensiones=todas_extensiones,
            max_iteraciones=50,
        )

        if not resultado_links.get("success"):
            scraper.close()
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "Error al extraer enlaces desde la web",
                    }
                ),
                500,
            )

        resultado_descarga = scraper.descargar_pdfs(json_path, UPLOAD_FOLDER)
        scraper.close()

        archivos = Funciones.listar_archivos_carpeta(
            UPLOAD_FOLDER, lista_tipos_archivos
        )

        return jsonify(
            {
                "success": True,
                "archivos": archivos,
                "mensaje": f"Se descargaron {len(archivos)} archivos",
                "stats": {
                    "total_enlaces": resultado_links.get("total_links", 0),
                    "descargados": resultado_descarga.get("descargados", 0),
                    "errores": resultado_descarga.get("errores", 0),
                },
            }
        )

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# -------------------- ZIP con JSON → Elastic --------------------

@app.route("/procesar-zip-elastic", methods=["POST"])
def procesar_zip_elastic():
    """
    Sube un ZIP con JSONs, lo descomprime en static/uploads y
    devuelve la lista de JSON encontrados.
    """
    ok, resp = require_login_y_permiso("admin_data_elastic")
    if not ok:
        return jsonify({"success": False, "error": "No autorizado"}), resp.status_code

    try:
        if "file" not in request.files:
            return (
                jsonify({"success": False, "error": "No se envió ningún archivo"}),
                400,
            )

        file = request.files["file"]
        if not file.filename:
            return (
                jsonify({"success": False, "error": "Archivo no válido"}),
                400,
            )

        Funciones.crear_carpeta(UPLOAD_FOLDER)
        Funciones.borrar_contenido_carpeta(UPLOAD_FOLDER)

        filename = secure_filename(file.filename)
        zip_path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(zip_path)

        # Descomprimir
        Funciones.descomprimir_zip_local(zip_path, UPLOAD_FOLDER)
        os.remove(zip_path)

        archivos_json = Funciones.listar_archivos_json(UPLOAD_FOLDER)

        return jsonify(
            {
                "success": True,
                "archivos": archivos_json,
                "mensaje": f"Se encontraron {len(archivos_json)} archivos JSON",
            }
        )

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/cargar-documentos-elastic", methods=["POST"])
def cargar_documentos_elastic():
    """
    Carga documentos (por ejemplo JSON) al índice indicado de Elasticsearch.
    Se usa después de procesar ZIP o WebScraping.
    """
    ok, resp = require_login_y_permiso("admin_data_elastic")
    if not ok:
        return jsonify({"success": False, "error": "No autorizado"}), resp.status_code

    try:
        data = request.get_json(silent=True) or {}

        archivos = data.get("archivos", [])
        index = data.get("index") or ELASTIC_INDEX_MINMINAS
        metodo = data.get("metodo", "zip")  # 'zip' o 'webscraping'

        if not archivos:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "No se enviaron archivos para procesar",
                    }
                ),
                400,
            )

        documentos: List[Dict[str, Any]] = []

        if metodo == "zip":
            # Archivos JSON que ya traen estructura lista para indexar
            for archivo in archivos:
                ruta = archivo.get("ruta")
                if ruta and os.path.exists(ruta):
                    doc = Funciones.leer_json(ruta)
                    if doc:
                        documentos.append(doc)

        elif metodo == "webscraping":
            # Procesar PDFs/TXT descargados → extraer texto y generar doc simple
            for archivo in archivos:
                ruta = archivo.get("ruta")
                extension = archivo.get("extension", "").lower()
                nombre_archivo = archivo.get("nombre")

                if not ruta or not os.path.exists(ruta):
                    continue

                texto = ""
                if extension == "pdf":
                    texto = Funciones.extraer_texto_pdf(ruta)
                    if not texto or len(texto.strip()) < 100:
                        texto = Funciones.extraer_texto_pdf_ocr(ruta)
                elif extension == "txt":
                    try:
                        with open(ruta, "r", encoding="utf-8") as f:
                            texto = f.read()
                    except Exception:
                        try:
                            with open(ruta, "r", encoding="latin-1") as f:
                                texto = f.read()
                        except Exception:
                            texto = ""

                if not texto or len(texto.strip()) < 50:
                    continue

                documento = {
                    "texto": texto,
                    "fecha": datetime.now().strftime("%Y-%m-%d"),
                    "ruta": ruta,
                    "nombre_archivo": nombre_archivo,
                }
                documentos.append(documento)

        if not documentos:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "No se pudieron generar documentos para indexar",
                    }
                ),
                400,
            )

        resultado = elastic.indexar_bulk(index=index, documentos=documentos)

        return jsonify(
            {
                "success": resultado.get("success", False),
                "indexados": resultado.get("indexados", 0),
                "errores": resultado.get("fallidos", 0),
            }
        )

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("VERIFICANDO CONEXIONES A SERVICIOS EXTERNOS")

    if mongo_users.test_connection():
        print("✅ MongoDB Atlas: Conectado")
    else:
        print("❌ MongoDB Atlas: Error de conexión")

    if elastic.test_connection():
        print("✅ Elasticsearch Cloud: Conectado")
    else:
        print("❌ Elasticsearch Cloud: Error de conexión")

    print("=" * 60 + "\n")

    app.run(debug=True, host="0.0.0.0", port=5000)
