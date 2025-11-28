import os
import zipfile
import requests
import json
import PyPDF2
from PIL import Image
import pytesseract
from typing import Dict, List
from datetime import datetime
import shutil  # para borrar directorios recursivamente


class Funciones:
    @staticmethod
    def crear_carpeta(ruta: str) -> bool:
        """Crea una carpeta si no existe."""
        try:
            if not os.path.exists(ruta):
                os.makedirs(ruta)
            return True
        except Exception as e:
            print(f"Error al crear carpeta: {e}")
            return False

    @staticmethod
    def descomprimir_zip_local(ruta_file_zip: str, ruta_descomprimir: str) -> List[Dict]:
        """
        Descomprime un archivo ZIP y retorna info de archivos.

        Solo procesa archivos con extensión .txt, .pdf y .json.
        """
        archivos: List[Dict] = []
        try:
            with zipfile.ZipFile(ruta_file_zip, 'r') as zip_ref:
                for file_info in zip_ref.namelist():
                    if not file_info.endswith('/'):
                        carpeta = os.path.dirname(file_info)
                        nombre_archivo = os.path.basename(file_info)
                        extension = os.path.splitext(nombre_archivo)[1].lower()

                        # Solo procesar txt, pdf y json
                        if extension in ['.txt', '.pdf', '.json']:
                            zip_ref.extract(file_info, ruta_descomprimir)
                            archivos.append({
                                'carpeta': carpeta if carpeta else 'raiz',
                                'nombre': nombre_archivo,
                                'ruta': os.path.join(ruta_descomprimir, file_info),
                                'extension': extension
                            })
            return archivos
        except Exception as e:
            print(f"Error al descomprimir ZIP: {e}")
            return []

    @staticmethod
    def descargar_y_descomprimir_zip(url: str, carpeta_destino: str, tipoArchivo: str = '') -> List[Dict]:
        """
        Descarga y descomprime un ZIP desde una URL.

        Args:
            url: URL del archivo ZIP.
            carpeta_destino: Carpeta donde se descargará y descomprimirá.
            tipoArchivo: (Reservado / no usado por ahora).

        Returns:
            Lista de diccionarios con información de los archivos extraídos.
        """
        try:
            Funciones.crear_carpeta(carpeta_destino)

            # Descargar archivo
            response = requests.get(url, stream=True)
            response.raise_for_status()

            zip_path = os.path.join(carpeta_destino, 'temp.zip')

            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            # Descomprimir
            archivos = Funciones.descomprimir_zip_local(zip_path, carpeta_destino)

            # Eliminar ZIP temporal
            os.remove(zip_path)

            return archivos
        except Exception as e:
            print(f"Error al descargar y descomprimir: {e}")
            return []

    @staticmethod
    def allowed_file(filename: str, extensions: List[str]) -> bool:
        """
        Verifica si un archivo tiene extensión permitida.

        Args:
            filename: Nombre del archivo.
            extensions: Lista de extensiones permitidas (sin el punto).

        Returns:
            True si está permitida, False en caso contrario.
        """
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in extensions

    @staticmethod
    def borrar_contenido_carpeta(ruta: str) -> bool:
        """
        Borra el contenido de una carpeta sin eliminar la carpeta misma.

        Args:
            ruta: Ruta de la carpeta a limpiar.

        Returns:
            True si se borró correctamente, False en caso de error.
        """
        try:
            if not os.path.exists(ruta):
                # Si no existe, no hay nada que borrar
                return True

            if not os.path.isdir(ruta):
                # No es una carpeta
                return False

            for item in os.listdir(ruta):
                item_path = os.path.join(ruta, item)
                try:
                    if os.path.isfile(item_path) or os.path.islink(item_path):
                        os.unlink(item_path)  # Eliminar archivo o enlace simbólico
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)  # Eliminar directorio y su contenido
                except Exception as e:
                    print(f"Error al eliminar {item_path}: {e}")
                    return False

            return True
        except Exception as e:
            print(f"Error al borrar contenido de carpeta: {e}")
            return False

    @staticmethod
    def extraer_texto_pdf(ruta_pdf: str) -> str:
        """
        Extrae texto de un archivo PDF usando PyPDF2.

        Args:
            ruta_pdf: Ruta del archivo PDF.

        Returns:
            Texto extraído del PDF (str).
        """
        try:
            texto = ""
            with open(ruta_pdf, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page in pdf_reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        texto += page_text + "\n"
            return texto.strip()
        except Exception as e:
            print(f"Error al extraer texto del PDF {ruta_pdf}: {e}")
            return ""

    @staticmethod
    def extraer_texto_pdf_ocr(ruta_pdf: str) -> str:
        """
        Extrae texto de un PDF usando OCR (útil para PDFs escaneados).

        Args:
            ruta_pdf: Ruta del archivo PDF.

        Returns:
            Texto extraído usando OCR (str).
        """
        try:
            from pdf2image import convert_from_path

            images = convert_from_path(ruta_pdf)

            texto = ""
            for image in images:
                texto += pytesseract.image_to_string(image, lang='spa') + "\n"

            return texto.strip()
        except Exception as e:
            print(f"Error al extraer texto con OCR del PDF {ruta_pdf}: {e}")
            return ""

    @staticmethod
    def listar_archivos_json(ruta_carpeta: str) -> List[Dict]:
        """
        Lista todos los archivos JSON en una carpeta.

        Args:
            ruta_carpeta: Ruta de la carpeta a explorar.

        Returns:
            Lista de diccionarios con información de cada archivo JSON.
        """
        archivos_json: List[Dict] = []
        try:
            if not os.path.exists(ruta_carpeta):
                return []

            for archivo in os.listdir(ruta_carpeta):
                if archivo.lower().endswith('.json'):
                    ruta_completa = os.path.join(ruta_carpeta, archivo)
                    archivos_json.append({
                        'nombre': archivo,
                        'ruta': ruta_completa,
                        'tamaño': os.path.getsize(ruta_completa)
                    })

            return archivos_json
        except Exception as e:
            print(f"Error al listar archivos JSON: {e}")
            return []

    @staticmethod
    def listar_archivos_carpeta(ruta_carpeta: str, extensiones: List[str] = None) -> List[Dict]:
        """
        Lista archivos en una carpeta con extensiones específicas.

        Args:
            ruta_carpeta: Ruta de la carpeta.
            extensiones: Lista de extensiones a filtrar (ej: ['pdf', 'txt'] sin punto).

        Returns:
            Lista de diccionarios con información de archivos.
        """
        archivos: List[Dict] = []
        try:
            if not os.path.exists(ruta_carpeta):
                return []

            for archivo in os.listdir(ruta_carpeta):
                ruta_completa = os.path.join(ruta_carpeta, archivo)
                if os.path.isfile(ruta_completa):
                    extension = os.path.splitext(archivo)[1].lower().replace('.', '')

                    if extensiones is None or extension in extensiones:
                        archivos.append({
                            'nombre': archivo,
                            'ruta': ruta_completa,
                            'extension': extension,
                            'tamaño': os.path.getsize(ruta_completa)
                        })

            return archivos
        except Exception as e:
            print(f"Error al listar archivos: {e}")
            return []

    @staticmethod
    def leer_json(ruta_json: str) -> Dict:
        """
        Lee un archivo JSON y retorna su contenido.

        Args:
            ruta_json: Ruta del archivo JSON.

        Returns:
            Diccionario con el contenido del JSON.
        """
        try:
            with open(ruta_json, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error al leer JSON {ruta_json}: {e}")
            return {}

    @staticmethod
    def guardar_json(ruta_json: str, datos: Dict) -> bool:
        """
        Guarda datos en un archivo JSON.

        Args:
            ruta_json: Ruta donde guardar el JSON.
            datos: Datos a guardar.

        Returns:
            True si se guardó correctamente, False si hubo error.
        """
        try:
            directorio = os.path.dirname(ruta_json)
            if directorio:
                Funciones.crear_carpeta(directorio)

            with open(ruta_json, 'w', encoding='utf-8') as f:
                json.dump(datos, f, indent=4, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error al guardar JSON: {e}")
            return False
