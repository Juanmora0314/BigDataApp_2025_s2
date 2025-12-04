# crear_admin.py
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from werkzeug.security import generate_password_hash

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "minminas_app")
MONGO_COLECCION = os.getenv("MONGO_COLECCION", "usuarios")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
col = db[MONGO_COLECCION]

username = "admin"
email = "admin@example.com"
password_plano = "Admin123*"

doc = col.find_one({"username": username})

if doc:
    print("⚠️ Ya existe un usuario 'admin'. No se creó uno nuevo.")
else:
    col.insert_one(
        {
            "username": username,
            "email": email,
            "rol": "admin",
            "password": generate_password_hash(password_plano),
            "activo": True,
        }
    )
    print("✅ Usuario admin creado:")
    print(f"  usuario: {username}")
    print(f"  contraseña: {password_plano}")
