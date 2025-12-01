"""
Módulo: elastic.py
Cliente de Elasticsearch Cloud para la app de MinMinas.

Requiere variables de entorno (por ejemplo en .env):

ELASTIC_CLOUD_ID=minminas-normatividad-search:...
ELASTIC_API_KEY=XXXXXXXXXXXX
ELASTIC_INDEX_DEFAULT=minminas-normatividad
"""

import os
import json
from typing import Dict, List, Optional, Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# ================== Carga de variables de entorno ==================

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Si no está instalado python-dotenv, se asume que las env vars ya existen
    pass

ELASTIC_CLOUD_ID = os.getenv("ELASTIC_CLOUD_ID")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")
ELASTIC_INDEX_DEFAULT = os.getenv("ELASTIC_INDEX_DEFAULT", "minminas-normatividad")

if not ELASTIC_CLOUD_ID or not ELASTIC_API_KEY:
    raise RuntimeError(
        "Faltan ELASTIC_CLOUD_ID o ELASTIC_API_KEY en las variables de entorno. "
        "Configúralas en tu archivo .env o en el entorno del sistema."
    )


class ElasticSearch:
    """
    Cliente de alto nivel para Elasticsearch Cloud.

    Uso típico:

        es = ElasticSearch()  # usa valores del .env
        es.test_connection()
        es.indexar_documento("minminas-normatividad", {...})
        res = es.buscar_texto("minminas-normatividad", "hidrocarburos", ["texto"])
    """

    def __init__(
        self,
        cloud_id: Optional[str] = None,
        api_key: Optional[str] = None,
        default_index: Optional[str] = None,
        request_timeout: int = 60,
    ):
        """
        Inicializa conexión a Elasticsearch Cloud.

        Args:
            cloud_id: Cloud ID del deployment (si None, usa ELASTIC_CLOUD_ID)
            api_key: API Key para autenticación (si None, usa ELASTIC_API_KEY)
            default_index: Índice por defecto (si None, usa ELASTIC_INDEX_DEFAULT)
            request_timeout: Timeout en segundos para peticiones (bulk/búsqueda)
        """
        self.cloud_id = cloud_id or ELASTIC_CLOUD_ID
        self.api_key = api_key or ELASTIC_API_KEY
        self.default_index = default_index or ELASTIC_INDEX_DEFAULT

        self.client = Elasticsearch(
            cloud_id=self.cloud_id,
            api_key=self.api_key,
            request_timeout=request_timeout,
        )

    # ----------------- TEST -----------------
    def test_connection(self) -> bool:
        """Prueba la conexión a Elasticsearch."""
        try:
            info = self.client.info()
            version = info.get("version", {}).get("number", "desconocida")
            print(f"✅ Conectado a Elasticsearch. Versión: {version}")
            print(f"   Índice por defecto: {self.default_index}")
            return True
        except Exception as e:
            print(f"❌ Error al conectar con Elasticsearch: {e}")
            return False

    # ----------------- ADMIN / ÍNDICES -----------------
    def ejecutar_comando(self, comando_json: str) -> Dict[str, Any]:
        """
        Ejecuta comandos administrativos (crear/eliminar índice, mappings, etc.)
        a partir de un JSON string.

        Estructura esperada, por ejemplo:

        {
            "operacion": "crear_index",
            "index": "minminas-normatividad",
            "mappings": {...},
            "settings": {...}
        }
        """
        try:
            comando = json.loads(comando_json)
            operacion = comando.get("operacion")
            index = comando.get("index", self.default_index)

            if operacion == "crear_index":
                mappings = comando.get("mappings", {})
                settings = comando.get("settings", {})
                response = self.client.indices.create(
                    index=index,
                    mappings=mappings,
                    settings=settings,
                )
                return {"success": True, "data": response}

            elif operacion == "eliminar_index":
                response = self.client.indices.delete(index=index)
                return {"success": True, "data": response}

            elif operacion == "actualizar_mappings":
                mappings = comando.get("mappings", {})
                response = self.client.indices.put_mapping(
                    index=index,
                    body=mappings,
                )
                return {"success": True, "data": response}

            elif operacion == "info_index":
                response = self.client.indices.get(index=index)
                return {"success": True, "data": response}

            elif operacion == "listar_indices":
                response = self.client.cat.indices(format="json")
                return {"success": True, "data": response}

            else:
                return {
                    "success": False,
                    "error": f"Operación no soportada: {operacion}",
                }

        except json.JSONDecodeError as e:
            return {"success": False, "error": f"JSON inválido: {str(e)}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def crear_index(
        self,
        nombre_index: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Crea un índice nuevo.

        Args:
            nombre_index: Nombre del índice.
            mappings: Definición de campos (mappings).
            settings: Configuración del índice (shards, analizador, etc.).
        """
        try:
            self.client.indices.create(
                index=nombre_index,
                mappings=mappings or {},
                settings=settings or {},
            )
            return True
        except Exception as e:
            print(f"[ElasticSearch] Error al crear índice '{nombre_index}': {e}")
            return False

    def eliminar_index(self, nombre_index: str) -> bool:
        """Elimina un índice."""
        try:
            self.client.indices.delete(index=nombre_index)
            return True
        except Exception as e:
            print(f"[ElasticSearch] Error al eliminar índice '{nombre_index}': {e}")
            return False

    def listar_indices(self) -> List[Dict[str, Any]]:
        """Lista índices con información básica (docs, tamaño, salud, estado)."""
        try:
            indices_raw = self.client.cat.indices(
                format="json",
                h="index,docs.count,store.size,health,status",
            )
            indices: List[Dict[str, Any]] = []
            for idx in indices_raw:
                docs_count_raw = str(idx.get("docs.count", "0"))
                try:
                    docs_count = int(docs_count_raw)
                except ValueError:
                    docs_count = 0

                indices.append(
                    {
                        "nombre": idx.get("index", ""),
                        "total_documentos": docs_count,
                        "tamaño": idx.get("store.size", "0b"),
                        "salud": idx.get("health", "unknown"),
                        "estado": idx.get("status", "unknown"),
                    }
                )
            return indices
        except Exception as e:
            print(f"[ElasticSearch] Error al listar índices: {e}")
            return []

    # ----------------- INDEXACIÓN -----------------
    def indexar_documento(
        self,
        index: Optional[str],
        documento: Dict[str, Any],
        doc_id: Optional[str] = None,
    ) -> bool:
        """
        Indexa un solo documento.

        Args:
            index: Nombre del índice (si None, usa índice por defecto).
            documento: Diccionario con los campos.
            doc_id: ID opcional para el documento (si None, Elastic genera uno).
        """
        idx = index or self.default_index
        try:
            if doc_id:
                self.client.index(index=idx, id=doc_id, document=documento)
            else:
                self.client.index(index=idx, document=documento)
            return True
        except Exception as e:
            print(f"[ElasticSearch] Error al indexar documento en '{idx}': {e}")
            return False

    def indexar_bulk(
        self,
        index: Optional[str],
        documentos: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Indexa documentos de forma masiva (bulk).

        Args:
            index: Nombre del índice (si None, usa índice por defecto).
            documentos: Lista de documentos (dict) a indexar.

        Returns:
            Dict con estadísticas de indexación: indexados, fallidos, errores.
        """
        idx = index or self.default_index

        try:
            acciones = [
                {
                    "_index": idx,
                    "_source": doc,
                }
                for doc in documentos
            ]

            success, errors = bulk(
                self.client,
                acciones,
                raise_on_error=False,
            )

            return {
                "success": True,
                "indexados": success,
                "fallidos": len(errors) if errors else 0,
                "errores": errors or [],
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
            }

    # ----------------- BÚSQUEDA -----------------
    def buscar(
        self,
        index: Optional[str],
        query: Dict[str, Any],
        aggs: Optional[Dict[str, Any]] = None,
        size: int = 10,
    ) -> Dict[str, Any]:
        """
        Realiza una búsqueda genérica en Elasticsearch.

        Args:
            index: Nombre del índice (si None, usa índice por defecto).
            query: Diccionario con la query de Elastic (debe incluir "query": {...}).
            aggs: Agregaciones opcionales.
            size: Número de resultados a devolver.

        Returns:
            Dict con success, total, resultados, aggs o error.
        """
        idx = index or self.default_index

        try:
            body = query.copy() if query else {}

            if aggs:
                body["aggs"] = aggs

            if size is not None:
                body["size"] = size

            response = self.client.search(index=idx, body=body)

            total_raw = response.get("hits", {}).get("total", {})
            if isinstance(total_raw, dict):
                total = int(total_raw.get("value", 0))
            else:
                total = int(total_raw) if total_raw is not None else 0

            return {
                "success": True,
                "total": total,
                "resultados": response.get("hits", {}).get("hits", []),
                "aggs": response.get("aggregations", {}),
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
            }

    def buscar_texto(
        self,
        index: Optional[str],
        texto: str,
        campos: Optional[List[str]] = None,
        size: int = 10,
    ) -> Dict[str, Any]:
        """
        Búsqueda de texto libre en uno o varios campos.

        Args:
            index: Nombre del índice (si None, usa índice por defecto).
            texto: Texto a buscar.
            campos: Lista de campos donde buscar (si None, usa query_string).
            size: Número de resultados.
        """
        if campos:
            query = {
                "query": {
                    "multi_match": {
                        "query": texto,
                        "fields": campos,
                        "type": "best_fields",
                    }
                }
            }
        else:
            query = {
                "query": {
                    "query_string": {
                        "query": texto,
                    }
                }
            }
        return self.buscar(index=index, query=query, size=size)

    # ----------------- CRUD DOCUMENTOS -----------------
    def obtener_documento(self, index: Optional[str], doc_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene un documento por ID."""
        idx = index or self.default_index
        try:
            response = self.client.get(index=idx, id=doc_id)
            return response.get("_source", {})
        except Exception as e:
            print(f"[ElasticSearch] Error al obtener documento {doc_id}: {e}")
            return None

    def actualizar_documento(
        self,
        index: Optional[str],
        doc_id: str,
        datos: Dict[str, Any],
    ) -> bool:
        """Actualiza un documento existente."""
        idx = index or self.default_index
        try:
            self.client.update(index=idx, id=doc_id, doc=datos)
            return True
        except Exception as e:
            print(f"[ElasticSearch] Error al actualizar documento {doc_id}: {e}")
            return False

    def eliminar_documento(self, index: Optional[str], doc_id: str) -> bool:
        """Elimina un documento por ID."""
        idx = index or self.default_index
        try:
            self.client.delete(index=idx, id=doc_id)
            return True
        except Exception as e:
            print(f"[ElasticSearch] Error al eliminar documento {doc_id}: {e}")
            return False

    def close(self):
        """Cierra la conexión con Elasticsearch."""
        self.client.close()


# ================== Bloque de prueba opcional ==================

if __name__ == "__main__":
    """
    Prueba rápida desde terminal:

    PS> cd C:\Users\...\BigDataApp_2025_s2
    PS> python -m Helpers.elastic
    """
    print("Probando conexión a Elasticsearch (MinMinas)...\n")

    es = ElasticSearch()

    if not es.test_connection():
        print("\n❌ No se pudo conectar a Elasticsearch. Revisa ELASTIC_CLOUD_ID / ELASTIC_API_KEY.")
    else:
        print("\nÍndices disponibles:")
        for idx in es.listar_indices():
            print(
                f" - {idx['nombre']} | docs={idx['total_documentos']} | "
                f"tamaño={idx['tamaño']} | salud={idx['salud']} | estado={idx['estado']}"
            )

    es.close()
    print("\nConexión cerrada.")
