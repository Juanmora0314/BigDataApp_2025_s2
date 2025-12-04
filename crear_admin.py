import os
from getpass import getpass

from dotenv import load_dotenv
from pymongo import MongoClient
from werkzeug.security import generate_password_hash

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "minminas_app")
MONGO_COLECCION = os.getenv("MONGO_COLECCION", "usuarios")


def main():
    if not MONGO_URI:
        print("❌ No hay MONGO_URI en el .env")
        return

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COLECCION]

    print("=== Crear usuario ADMIN ===")
    username = input("Usuario: ").strip()
    email = input("Correo: ").strip()

    if col.find_one({"$or": [{"username": username}, {"email": email}]}):
        print("⚠️ Ya existe un usuario con ese username o correo.")
        return

    pwd1 = getpass("Contraseña: ")
    pwd2 = getpass("Confirma la contraseña: ")
    if pwd1 != pwd2:
        print("❌ Las contraseñas no coinciden.")
        return

    hashed = generate_password_hash(pwd1)

    user_doc = {
        "username": username,
        "email": email,
        "password": hashed,
        "rol": "admin",
        "activo": True,
        "ultimo_acceso": None,
    }

    col.insert_one(user_doc)
    print("✅ Usuario admin creado correctamente.")


if __name__ == "__main__":
    main()
