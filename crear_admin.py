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
usuarios = db[MONGO_COLECCION]

username = "admin"
email = "admin@minminas.gov.co"
password_plano = "Admin2025*"

doc = usuarios.find_one({"$or": [{"username": username}, {"email": email}]})

if doc:
    print("⚠️ Ya existe un usuario con ese username o email.")
else:
    hashed = generate_password_hash(password_plano)
    usuarios.insert_one(
        {
            "username": username,
            "email": email,
            "password": hashed,
            "rol": "admin",
            "activo": True,
        }
    )
    print("✅ Usuario admin creado.")
    print(f"   Usuario: {username}")
    print(f"   Email:   {email}")
    print(f"   Clave:   {password_plano}")
