"""
Módulo: mongoDB.py
Gestión de usuarios en MongoDB para la app de MinMinas.

Requiere variables de entorno (por ejemplo en .env):

MONGO_URI=mongodb+srv://JuanMora:MoraJuan@cluster0.lbbzisf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0
MONGO_DB=minminas_app
MONGO_COLECCION=usuarios
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Any

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from bson import ObjectId
from passlib.hash import bcrypt

# ================== Carga de variables de entorno ==================

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Si no está instalado python-dotenv, se asume que las env vars ya existen
    pass

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "minminas_app")
MONGO_COLECCION = os.getenv("MONGO_COLECCION", "usuarios")

if not MONGO_URI:
    raise RuntimeError(
        "La variable de entorno MONGO_URI no está definida. "
        "Configúrala en tu .env o en el entorno del sistema."
    )


class MongoDBUsuarios:
    """
    Clase de gestión de usuarios en MongoDB para la app MinMinas.

    Esquema esperado en la colección 'usuarios':

    {
        _id: ObjectId,
        username: str,
        email: str,
        password_hash: str,   # bcrypt
        rol: str,             # admin / analista / invitado, etc.
        activo: bool,
        created_at: datetime,
        updated_at: datetime,
        ultimo_login: datetime | None
    }
    """

    def __init__(self, uri: str, db_name: str, coleccion: str = "usuarios"):
        """Inicializa conexión a MongoDB."""
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.col = self.db[coleccion]

        # Crear índices únicos en email y username
        self.col.create_index([("email", ASCENDING)], unique=True)
        self.col.create_index([("username", ASCENDING)], unique=True)

    # ------------------------------------------------------------------
    # Conexión
    # ------------------------------------------------------------------
    def test_connection(self) -> bool:
        """Prueba la conexión a MongoDB."""
        try:
            self.client.admin.command("ping")
            return True
        except ConnectionFailure:
            return False

    # ------------------------------------------------------------------
    # CRUD / autenticación
    # ------------------------------------------------------------------
    def crear_usuario(
        self,
        username: str,
        email: str,
        password: str,
        rol: str = "analista",
        activo: bool = True
    ) -> Optional[str]:
        """
        Crea un nuevo usuario con contraseña hasheada (bcrypt).
        Retorna el _id como str o None si hay error (por ej. duplicado).
        """
        ahora = datetime.utcnow()
        doc = {
            "username": username.strip().lower(),
            "email": email.strip().lower(),
            "password_hash": bcrypt.hash(password),
            "rol": rol,
            "activo": activo,
            "created_at": ahora,
            "updated_at": ahora,
            "ultimo_login": None
        }

        try:
            res = self.col.insert_one(doc)
            return str(res.inserted_id)
        except DuplicateKeyError as e:
            print(f"[MongoDBUsuarios] Usuario/email duplicado: {e}")
            return None
        except Exception as e:
            print(f"[MongoDBUsuarios] Error al crear usuario: {e}")
            return None

    def validar_usuario(self, usuario_o_email: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Valida usuario y contraseña:
        - Permite login tanto por username como por email.
        - Solo usuarios activos.

        Retorna el documento de usuario normalizado (incluye 'id' en vez de '_id')
        o None si usuario/contraseña no coinciden.
        """
        try:
            filtro = {
                "activo": True,
                "$or": [
                    {"username": usuario_o_email.strip().lower()},
                    {"email": usuario_o_email.strip().lower()}
                ]
            }
            user = self.col.find_one(filtro)
            if not user:
                return None

            if not bcrypt.verify(password, user["password_hash"]):
                return None

            # Actualizar último login
            self.col.update_one(
                {"_id": user["_id"]},
                {"$set": {
                    "ultimo_login": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }}
            )

            # Normalizar _id → id
            user_norm = dict(user)
            user_norm["id"] = str(user_norm.pop("_id"))
            return user_norm

        except Exception as e:
            print(f"[MongoDBUsuarios] Error al validar usuario: {e}")
            return None

    def obtener_usuario(self, username: str) -> Optional[Dict[str, Any]]:
        """Obtiene la información de un usuario por su username."""
        try:
            user = self.col.find_one({"username": username.strip().lower()})
            if not user:
                return None
            user_norm = dict(user)
            user_norm["id"] = str(user_norm.pop("_id"))
            return user_norm
        except Exception as e:
            print(f"[MongoDBUsuarios] Error al obtener usuario: {e}")
            return None

    def listar_usuarios(self, solo_activos: bool = False, limit: int = 100) -> List[Dict[str, Any]]:
        """Lista usuarios (por defecto hasta 100)."""
        try:
            filtro: Dict[str, Any] = {}
            if solo_activos:
                filtro["activo"] = True

            cursor = self.col.find(filtro).limit(limit)
            usuarios: List[Dict[str, Any]] = []
            for u in cursor:
                u_norm = dict(u)
                u_norm["id"] = str(u_norm.pop("_id"))
                usuarios.append(u_norm)
            return usuarios
        except Exception as e:
            print(f"[MongoDBUsuarios] Error al listar usuarios: {e}")
            return []

    def actualizar_usuario(self, user_id: str, nuevos_datos: Dict[str, Any]) -> bool:
        """
        Actualiza un usuario existente por _id.
        No permite cambiar password aquí (usar cambiar_password).
        """
        try:
            datos = dict(nuevos_datos)  # copia
            datos.pop("password_hash", None)
            datos.pop("password", None)
            datos["updated_at"] = datetime.utcnow()

            res = self.col.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": datos}
            )
            return res.matched_count == 1
        except Exception as e:
            print(f"[MongoDBUsuarios] Error al actualizar usuario: {e}")
            return False

    def cambiar_password(self, user_id: str, nueva_password: str) -> bool:
        """Actualiza la contraseña de un usuario (re-hash)."""
        try:
            hash_nuevo = bcrypt.hash(nueva_password)
            res = self.col.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": {
                    "password_hash": hash_nuevo,
                    "updated_at": datetime.utcnow()
                }}
            )
            return res.matched_count == 1
        except Exception as e:
            print(f"[MongoDBUsuarios] Error al cambiar password: {e}")
            return False

    def eliminar_usuario(self, user_id: str) -> bool:
        """Elimina definitivamente un usuario (hard delete)."""
        try:
            res = self.col.delete_one({"_id": ObjectId(user_id)})
            return res.deleted_count > 0
        except Exception as e:
            print(f"[MongoDBUsuarios] Error al eliminar usuario: {e}")
            return False

    def desactivar_usuario(self, user_id: str) -> bool:
        """Desactiva (soft delete) un usuario."""
        try:
            res = self.col.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": {
                    "activo": False,
                    "updated_at": datetime.utcnow()
                }}
            )
            return res.matched_count == 1
        except Exception as e:
            print(f"[MongoDBUsuarios] Error al desactivar usuario: {e}")
            return False

    def close(self):
        """Cierra la conexión."""
        self.client.close()


# ================== Bloque de prueba opcional ==================

if __name__ == "__main__":
    print("Probando conexión y CRUD básico de usuarios...\n")

    mongo_users = MongoDBUsuarios(
        uri=MONGO_URI,
        db_name=MONGO_DB,
        coleccion=MONGO_COLECCION,
    )

    print("Conexión OK? ->", mongo_users.test_connection())

    # Crear usuario de prueba
    uid = mongo_users.crear_usuario(
        username="minminas_test",
        email="minminas_test@example.com",
        password="Secreto123!",
        rol="admin"
    )
    print("ID usuario creado:", uid)

    # Intentar login
    if uid:
        print("\nIntentando login con username...")
        u_login = mongo_users.validar_usuario("minminas_test", "Secreto123!")
        print("Resultado login:", u_login)

    # Listar usuarios
    print("\nUsuarios en la colección:")
    for u in mongo_users.listar_usuarios(limit=10):
        print(u)

    # Cerrar conexión
    mongo_users.close()
    print("\nConexión cerrada.")
