"""
Módulo: webScrapingMinMinas.py
Web-scraping del repositorio normativo del Ministerio de Minas y Energía
y carga de documentos en Elasticsearch.

Pipeline:
  1. Crawler sobre https://www.minenergia.gov.co/es/repositorio-normativo/normativa/
  2. Descarga de PDFs (hasta MAX_PDFS).
  3. Extracción de texto (usa Funciones.extraer_texto_pdf y OCR como respaldo).
  4. Indexación en Elasticsearch (índice ELASTIC_INDEX_MINMINAS).
  5. Generación de estadísticas en JSON.
"""

import os
import time
import json
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Imports internos del proyecto
try:
    from Helpers.funciones import Funciones
    from Helpers.elastic import ElasticSearch
except ImportError:
    # Por si se ejecuta como paquete (python -m Helpers.webScrapingMinMinas)
    from .funciones import Funciones
    from .elastic import ElasticSearch

# .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class WebScrapingMinMinas:
    """
    Clase que implementa todo el flujo de scraping + indexación en Elastic
    para el repositorio normativo del Ministerio de Minas y Energía.
    """

    BASE_URL = "https://www.minenergia.gov.co/es/repositorio-normativo/normativa/"

    def __init__(
        self,
        base_dir: Optional[str] = None,
        max_pdfs: int = 200,
        max_pages: int = 100,
    ):
        # Directorios base donde se guardan PDFs, stats, etc.
        if base_dir is None:
            # Carpeta base relativa al proyecto (ajústala si quieres otra ruta)
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            base_dir = os.path.join(project_root, "Files_WebScrapping_MinMinas")

        self.base_dir = base_dir
        self.pdfs_dir = os.path.join(self.base_dir, "01_PDFs_Descargados")
        self.stats_path = os.path.join(self.base_dir, "estadisticas_minminas.json")

        # Crear carpetas
        Funciones.crear_carpeta(self.base_dir)
        Funciones.crear_carpeta(self.pdfs_dir)

        # Límites de crawler
        self.max_pdfs = max_pdfs
        self.max_pages = max_pages

        # Listas usadas en el pipeline
        self.pdf_links: List[Dict] = []
        self.downloaded_pdfs: List[Dict] = []
        self.failed_downloads: List[Dict] = []
        self.failed_extractions: List[Dict] = []

        # ---------- Configuración de Elasticsearch ----------
        self.cloud_id = os.getenv("ELASTIC_CLOUD_ID")
        self.api_key = os.getenv("ELASTIC_API_KEY")
        self.index_name = os.getenv("ELASTIC_INDEX_MINMINAS", "minminas-normatividad")

        if not self.cloud_id or not self.api_key:
            raise RuntimeError(
                "ELASTIC_CLOUD_ID o ELASTIC_API_KEY no están definidos en .env"
            )

        # Cliente Elastic (usando tu clase Helpers.elastic.ElasticSearch)
        self.es = ElasticSearch(
            cloud_id=self.cloud_id,
            api_key=self.api_key,
            default_index=self.index_name,
        )

    # ------------------------------------------------------------------
    # 1. CRAWLER: extraer links desde una página
    # ------------------------------------------------------------------
    def extract_links_from_page(self, url: str) -> List[Dict]:
        """
        Extrae links PDF y páginas internas del repositorio normativo
        del Ministerio de Minas y Energía.

        Devuelve una lista de diccionarios con:
          - 'url'
          - 'text'
          - 'type' = 'pdf' o 'page'
        """
        links_found: List[Dict] = []

        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "lxml")

            for a in soup.find_all("a", href=True):
                href = a.get("href", "").strip()
                text = a.get_text(strip=True)

                if not href:
                    continue

                # Ignorar mailto, tel, etc.
                if href.startswith("mailto:") or href.startswith("tel:"):
                    continue

                # URL absoluta
                full_url = urljoin(url, href)
                full_url = full_url.split("#")[0]  # quitar anclas

                if "minenergia.gov.co" not in full_url:
                    continue

                low = full_url.lower()

                # Enlace a PDF
                if ".pdf" in low:
                    links_found.append(
                        {
                            "url": full_url,
                            "text": text,
                            "type": "pdf",
                        }
                    )
                    continue

                # Página interna del repositorio
                if "/repositorio-normativo/" in low:
                    links_found.append(
                        {
                            "url": full_url,
                            "text": text,
                            "type": "page",
                        }
                    )

        except requests.exceptions.RequestException as e:
            print(f"[extract_links_from_page] Error en {url}: {e}")

        return links_found

    # ------------------------------------------------------------------
    # 2. CRAWLER: recorrer varias páginas hasta reunir N PDFs
    # ------------------------------------------------------------------
    def crawl_minminas(self) -> List[Dict]:
        """
        Crawler BFS sobre el repositorio normativo de MinMinas.
        Llena self.pdf_links con hasta self.max_pdfs PDFs.
        """
        print("=" * 70)
        print("CRAWLER MINMINAS - REPOSITORIO NORMATIVO")
        print("=" * 70)
        print()

        visited_urls = set()
        to_visit = [self.BASE_URL]
        pdf_links: List[Dict] = []

        pages_explored = 0

        while (
            to_visit
            and len(pdf_links) < self.max_pdfs
            and pages_explored < self.max_pages
        ):
            current_url = to_visit.pop(0)

            if current_url in visited_urls:
                continue

            visited_urls.add(current_url)
            pages_explored += 1

            print(
                f"[{pages_explored}/{self.max_pages}] Explorando página: {current_url}"
            )

            links = self.extract_links_from_page(current_url)

            for l in links:
                url_l = l.get("url", "")
                tipo = l.get("type", "")

                if tipo == "pdf":
                    pdf_links.append(l)
                elif tipo == "page":
                    if url_l not in visited_urls and url_l not in to_visit:
                        to_visit.append(url_l)

            print(f"    PDFs acumulados: {len(pdf_links)}\n")

        print("=" * 70)
        print(f"RESULTADO CRAWLER: {len(pdf_links)} PDFs encontrados (MinMinas)")
        print("=" * 70)
        print()

        # Cortar a max_pdfs por si acaso
        pdf_links = pdf_links[: self.max_pdfs]
        self.pdf_links = pdf_links
        return pdf_links

    # ------------------------------------------------------------------
    # 3. DESCARGA DE PDFs
    # ------------------------------------------------------------------
    def download_pdfs(self):
        """
        Descarga todos los PDFs de self.pdf_links a self.pdfs_dir.
        Llena self.downloaded_pdfs y self.failed_downloads.
        """
        if not self.pdf_links:
            print("No hay pdf_links cargados. Ejecuta crawl_minminas() primero.")
            return

        print("=" * 70)
        print("DESCARGANDO PDFs (MinMinas)")
        print("=" * 70)
        print()

        downloaded_pdfs: List[Dict] = []
        failed_downloads: List[Dict] = []

        for idx, pdf_info in enumerate(self.pdf_links, 1):
            pdf_url = pdf_info["url"]
            try:
                resp = requests.get(pdf_url, timeout=20, stream=True)
                resp.raise_for_status()

                # Nombre de archivo
                url_parts = urlparse(pdf_url)
                filename = os.path.basename(url_parts.path)

                if not filename or "." not in filename:
                    filename = f"minminas_{idx}.pdf"

                filepath = os.path.join(self.pdfs_dir, filename)

                with open(filepath, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                info_pdf = {
                    "filename": filename,
                    "filepath": filepath,
                    "url": pdf_url,
                    "size_bytes": os.path.getsize(filepath),
                    "download_date": datetime.utcnow().isoformat(),
                }
                downloaded_pdfs.append(info_pdf)

                print(f"[{idx}/{len(self.pdf_links)}] Descargado: {filename}")

            except Exception as e:
                failed_downloads.append(
                    {
                        "url": pdf_url,
                        "error": str(e),
                    }
                )
                print(f"[{idx}/{len(self.pdf_links)}] Error descargando: {pdf_url}")
                print("   ", e)

            time.sleep(0.5)

        print()
        print("=" * 70)
        print(
            f"Descarga completada: {len(downloaded_pdfs)} exitosos, {len(failed_downloads)} errores"
        )
        print("=" * 70)
        print()

        self.downloaded_pdfs = downloaded_pdfs
        self.failed_downloads = failed_downloads

    # ------------------------------------------------------------------
    # 4. EXTRAER TEXTO + INDEXAR EN ELASTIC
    # ------------------------------------------------------------------
    def process_and_index_pdfs(self):
        """
        Recorre self.downloaded_pdfs:
          - extrae texto de cada PDF (Funciones.extraer_texto_pdf, y OCR si quieres)
          - construye documentos y los indexa en Elasticsearch (bulk)
        """
        if not self.downloaded_pdfs:
            print("No hay PDFs descargados. Ejecuta download_pdfs() primero.")
            return

        print("=" * 70)
        print("EXTRAYENDO TEXTO E INDEXANDO EN ELASTIC (MinMinas)")
        print("=" * 70)
        print()

        documentos_elastic: List[Dict] = []
        failed_extractions: List[Dict] = []
        processed_count = 0

        for idx, pdf_info in enumerate(self.downloaded_pdfs, 1):
            pdf_path = pdf_info["filepath"]
            filename = pdf_info["filename"]

            print(f"[{idx}/{len(self.downloaded_pdfs)}] Procesando: {filename}")

            # 1) Intento de extracción de texto "normal"
            texto = Funciones.extraer_texto_pdf(pdf_path)

            # 2) Si viene vacío, podrías intentar OCR con extraer_texto_pdf_ocr
            if not texto.strip():
                try:
                    texto = Funciones.extraer_texto_pdf_ocr(pdf_path)
                except Exception as e:
                    print("   Error en OCR:", e)

            if texto.strip():
                doc = {
                    "nombre_archivo": filename,
                    "fecha": datetime.utcnow().strftime("%Y-%m-%d"),
                    "texto": texto,
                    "size_kb": round(pdf_info["size_bytes"] / 1024, 2),
                    "url": pdf_info["url"],
                    "fuente": "Minenergia - Repositorio Normativo",
                }
                documentos_elastic.append(doc)
                processed_count += 1
                print("   ✔ Texto extraído, listo para indexar")
            else:
                failed_extractions.append(
                    {
                        "filename": filename,
                        "filepath": pdf_path,
                        "error": "sin_texto",
                    }
                )
                print("   ✖ No se pudo extraer texto\n")

        # ---------- Indexar en Elastic (bulk) ----------
        print()
        print("Indexando en Elasticsearch (bulk) ...")
        resultado_bulk = self.es.indexar_bulk(
            index=self.index_name,
            documentos=documentos_elastic,
        )
        print("Resultado bulk:", resultado_bulk)

        self.failed_extractions = failed_extractions

        # ---------- Estadísticas ----------
        errores_descarga = len(self.failed_downloads)
        errores_extraccion = len(self.failed_extractions)
        errores_elastic = (
            0
            if resultado_bulk.get("success")
            else max(1, resultado_bulk.get("fallidos", 0))
        )

        try:
            count_resp = self.es.client.count(index=self.index_name)
            docs_en_elastic = count_resp["count"]
        except Exception:
            docs_en_elastic = resultado_bulk.get("indexados", processed_count)

        stats = {
            "fecha_proceso": datetime.utcnow().isoformat(),
            "pdfs_encontrados": len(self.pdf_links),
            "pdfs_descargados": len(self.downloaded_pdfs),
            "pdfs_procesados": processed_count,
            "documentos_elastic": docs_en_elastic,
            "errores_descarga": errores_descarga,
            "errores_extraccion": errores_extraccion,
            "errores_elastic": errores_elastic,
            "porcentaje_exito": round(
                (processed_count / len(self.pdf_links) * 100), 2
            )
            if self.pdf_links
            else 0,
        }

        Funciones.guardar_json(self.stats_path, stats)

        print()
        print("=" * 70)
        print("RESUMEN DEL PROCESO - MINMINAS / ELASTIC")
        print("=" * 70)
        print(f"PDFs encontrados (crawler): {stats['pdfs_encontrados']}")
        print(f"PDFs descargados:           {stats['pdfs_descargados']}")
        print(f"PDFs procesados (texto):    {stats['pdfs_procesados']}")
        print(f"Docs en Elasticsearch:      {stats['documentos_elastic']}")
        print("-" * 70)
        print(f"Errores en descarga:        {stats['errores_descarga']}")
        print(f"Errores en extracción:      {stats['errores_extraccion']}")
        print(f"Errores en Elastic:         {stats['errores_elastic']}")
        print("-" * 70)
        print(f"Porcentaje de éxito total:  {stats['porcentaje_exito']}%")
        print("-" * 70)
        print(f"Estadísticas guardadas en:  {self.stats_path}")
        print("=" * 70)
        print("PROCESO COMPLETADO - MINMINAS → ELASTIC")
        print("=" * 70)
        print()


# ----------------------------------------------------------------------
# Bloque de prueba: ejecutar todo el pipeline desde consola
# ----------------------------------------------------------------------
if __name__ == "__main__":
    print(">>> INICIANDO PIPELINE MINMINAS → ELASTIC <<<\n")

    scraper = WebScrapingMinMinas(
        base_dir=None,     # usa la ruta por defecto dentro del proyecto
        max_pdfs=200,
        max_pages=100,
    )

    # 1. Crawler
    scraper.crawl_minminas()

    # 2. Descarga de PDFs
    scraper.download_pdfs()

    # 3. Extraer texto + indexar en Elastic
    scraper.process_and_index_pdfs()

    print("\n>>> PIPELINE FINALIZADO <<<")
