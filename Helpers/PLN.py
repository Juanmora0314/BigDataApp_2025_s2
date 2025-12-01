"""
Módulo: PLN.py
Utilidades de Procesamiento de Lenguaje Natural (PLN) para la
aplicación de búsqueda de normatividad del Ministerio de Minas y Energía
(MinMinas).

La idea es usar esta clase para:
- Limpiar y normalizar el texto de los documentos normativos.
- Extraer entidades (organizaciones, fechas, leyes, etc.).
- Sacar palabras clave / temas principales.
- Generar resúmenes extractivos cortos.
- (Opcional) Calcular similitud semántica entre textos.
- (Opcional) Analizar sentimiento con modelos de transformers.

Varios componentes son OPCIONALES y están protegidos con try/except
para que el módulo no “reviente” si alguna librería pesada no está
instalada (sentence-transformers, transformers, sklearn, etc.).
"""

from __future__ import annotations

import warnings
warnings.filterwarnings("ignore")

from typing import List, Dict, Tuple, Optional

from collections import Counter
import re

# --- NLTK: stopwords ---
import nltk
from nltk.corpus import stopwords

# Intentamos asegurarnos de que estén los recursos de NLTK
try:
    nltk.download("stopwords", quiet=True)
except Exception:
    pass

# --- spaCy (obligatorio para la mayoría de métodos) ---
try:
    import spacy
except ImportError:
    spacy = None

# --- NumPy (para TF-IDF / similitud) ---
try:
    import numpy as np
except ImportError:
    np = None

# --- scikit-learn (TF-IDF, similitud de coseno) ---
try:
    from sklearn.metrics.pairwise import cosine_similarity
    from sklearn.feature_extraction.text import TfidfVectorizer
except ImportError:
    cosine_similarity = None
    TfidfVectorizer = None

# --- sentence-transformers (embeddings avanzados, opcional) ---
try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    SentenceTransformer = None

# --- transformers (sentiment, opcional) ---
try:
    from transformers import pipeline
except ImportError:
    pipeline = None

# --- pandas (para matriz de similitud, opcional) ---
try:
    import pandas as pd
except ImportError:
    pd = None


class PLN:
    """
    Clase de utilidades de PLN para trabajar con textos normativos de MinMinas.
    Pensada para usarse en:
      - vistas Flask / FastAPI
      - scripts de prueba
      - notebooks
    """

    def __init__(
        self,
        modelo_spacy: str = "es_core_news_sm",
        modelo_embeddings: str = "paraphrase-multilingual-MiniLM-L12-v2",
        cargar_modelos: bool = True,
    ):
        """
        Inicializa la clase PLN.

        Args:
            modelo_spacy:
                Nombre del modelo de spaCy (por ejemplo 'es_core_news_sm'
                o 'es_core_news_lg'). Debe estar previamente descargado.
            modelo_embeddings:
                Nombre del modelo de SentenceTransformer para similitud
                semántica (opcional).
            cargar_modelos:
                Si True, intenta cargar spaCy, embeddings y stopwords al
                crear la instancia.
        """
        self.modelo_spacy_nombre = modelo_spacy
        self.modelo_embeddings_nombre = modelo_embeddings

        self.nlp = None                 # modelo spaCy
        self.model_embeddings = None    # SentenceTransformer
        self.stopwords_es = set()

        if cargar_modelos:
            self._cargar_modelos_basicos()
            self._cargar_modelos_avanzados()

    # ------------------------------------------------------------------
    # CARGA DE MODELOS
    # ------------------------------------------------------------------
    def _cargar_modelos_basicos(self) -> None:
        """Carga spaCy y stopwords de NLTK (parte básica del PLN)."""
        # spaCy
        if spacy is None:
            print(
                "[PLN] spaCy no está instalado. "
                "Instala con: pip install spacy"
            )
            self.nlp = None
        else:
            try:
                print(f"[PLN] Cargando modelo spaCy '{self.modelo_spacy_nombre}'...")
                self.nlp = spacy.load(self.modelo_spacy_nombre)
                print("[PLN] Modelo spaCy cargado correctamente.")
            except OSError:
                print(
                    f"[PLN] No se encontró el modelo '{self.modelo_spacy_nombre}'. "
                    f"Descárgalo con: python -m spacy download {self.modelo_spacy_nombre}"
                )
                self.nlp = None

        # Stopwords en español
        try:
            self.stopwords_es = set(stopwords.words("spanish"))
        except LookupError:
            try:
                nltk.download("stopwords", quiet=True)
                self.stopwords_es = set(stopwords.words("spanish"))
            except Exception:
                self.stopwords_es = set()

    def _cargar_modelos_avanzados(self) -> None:
        """
        Carga modelos avanzados opcionales:
          - SentenceTransformer (embeddings)
        No carga transformers (sentiment) todavía, se cargan on-demand.
        """
        # Sentence-transformers
        if SentenceTransformer is None:
            print(
                "[PLN] sentence-transformers no está instalado. "
                "Instala con: pip install sentence-transformers"
            )
            self.model_embeddings = None
        else:
            try:
                print(
                    f"[PLN] Cargando modelo de embeddings "
                    f"'{self.modelo_embeddings_nombre}'..."
                )
                self.model_embeddings = SentenceTransformer(
                    self.modelo_embeddings_nombre
                )
                print("[PLN] Modelo de embeddings cargado correctamente.")
            except Exception as e:
                print(f"[PLN] Error al cargar modelo de embeddings: {e}")
                self.model_embeddings = None

    # ------------------------------------------------------------------
    # MÉTODOS PRINCIPALES
    # ------------------------------------------------------------------
    def _check_spacy(self):
        if self.nlp is None:
            raise RuntimeError(
                "El modelo de spaCy no está cargado. "
                "Asegúrate de haber instalado el modelo y de llamar a "
                "PLN(..., cargar_modelos=True) o a _cargar_modelos_basicos()."
            )

    # ---------- ENTIDADES ----------
    def extraer_entidades(self, texto: str) -> Dict[str, List[str]]:
        """
        Extrae entidades nombradas del texto usando spaCy.

        Pensado para textos normativos: detecta personas, lugares,
        organizaciones, fechas y posibles leyes.

        Returns:
            Diccionario con listas de entidades por tipo.
        """
        self._check_spacy()
        doc = self.nlp(texto)

        entidades: Dict[str, List[str]] = {
            "personas": [],
            "lugares": [],
            "organizaciones": [],
            "fechas": [],
            "leyes": [],
            "otros": [],
        }

        for ent in doc.ents:
            label = ent.label_
            text_ent = ent.text.strip()

            if label == "PER":
                entidades["personas"].append(text_ent)
            elif label in ("LOC", "GPE"):
                entidades["lugares"].append(text_ent)
            elif label == "ORG":
                entidades["organizaciones"].append(text_ent)
            elif label == "DATE":
                entidades["fechas"].append(text_ent)
            elif label == "LAW" or "ley" in text_ent.lower():
                entidades["leyes"].append(text_ent)
            else:
                entidades["otros"].append(f"{text_ent} ({label})")

        # Eliminar duplicados manteniendo orden
        for key in entidades:
            entidades[key] = list(dict.fromkeys(entidades[key]))

        return entidades

    # ---------- TEMAS / PALABRAS CLAVE ----------
    def extraer_temas(self, texto: str, top_n: int = 10) -> List[Tuple[str, float]]:
        """
        Extrae palabras clave / temas importantes del texto usando
        frecuencia de lemas.

        Devuelve una lista de (palabra, %_relevancia_entre_0_y_100).
        """
        self._check_spacy()
        doc = self.nlp(texto)

        palabras_relevantes: List[str] = []
        for token in doc:
            if (
                not token.is_stop
                and not token.is_punct
                and not token.is_space
                and len(token.text) > 3
                and token.pos_ in ("NOUN", "PROPN", "ADJ", "VERB")
            ):
                palabras_relevantes.append(token.lemma_.lower())

        contador = Counter(palabras_relevantes)
        temas = contador.most_common(top_n)

        total = len(palabras_relevantes)
        if total > 0:
            temas = [(pal, freq * 100.0 / total) for pal, freq in temas]
        else:
            temas = [(pal, 0.0) for pal, _ in temas]

        return temas

    # ---------- RESUMEN ----------
    def generar_resumen(self, texto: str, num_oraciones: int = 3) -> str:
        """
        Genera un resumen extractivo sencillo con TF-IDF:
        selecciona las oraciones más representativas.

        Si sklearn o numpy no están instalados, hace un resumen muy
        básico con las primeras N oraciones.
        """
        self._check_spacy()
        doc = self.nlp(texto)
        oraciones = [sent.text.strip() for sent in doc.sents if len(sent.text.strip()) > 20]

        if len(oraciones) == 0:
            return texto[:200] + "..." if len(texto) > 200 else texto

        if len(oraciones) <= num_oraciones:
            return " ".join(oraciones)

        # Si falta sklearn o numpy -> fallback simple
        if TfidfVectorizer is None or np is None:
            return " ".join(oraciones[:num_oraciones])

        try:
            vectorizer = TfidfVectorizer(stop_words=list(self.stopwords_es))
            tfidf_matrix = vectorizer.fit_transform(oraciones)

            puntuaciones = tfidf_matrix.sum(axis=1).A1  # vector 1D
            idx_importantes = puntuaciones.argsort()[-num_oraciones:][::-1]
            idx_importantes = sorted(idx_importantes)  # conservar orden original

            resumen = " ".join(oraciones[i] for i in idx_importantes)
            return resumen
        except Exception as e:
            print(f"[PLN] Error en generar_resumen: {e}")
            return " ".join(oraciones[:num_oraciones])

    # ---------- PREPROCESAMIENTO ----------
    def preprocesar_texto(
        self,
        texto: str,
        remover_stopwords: bool = True,
        lematizar: bool = True,
        remover_numeros: bool = False,
        min_longitud: int = 3,
    ) -> str:
        """
        Limpia y normaliza un texto (para indexar en Elastic, por ejemplo).

        - quita stopwords
        - elimina puntuación
        - puede eliminar números
        - lematiza o deja forma original
        """
        self._check_spacy()
        doc = self.nlp(texto)
        palabras: List[str] = []

        for token in doc:
            if len(token.text) < min_longitud:
                continue
            if remover_stopwords and token.is_stop:
                continue
            if token.is_punct or token.is_space:
                continue
            if remover_numeros and token.like_num:
                continue

            palabra = token.lemma_.lower() if lematizar else token.text.lower()
            palabras.append(palabra)

        return " ".join(palabras)

    # ---------- SIMILITUD SEMÁNTICA (OPCIONAL) ----------
    def calcular_similitud_semantica(self, textos: List[str]):
        """
        Calcula una matriz de similitud semántica entre textos usando
        embeddings de SentenceTransformer.

        Devuelve:
            - un DataFrame de pandas si está disponible
            - en caso contrario, una lista de listas (matriz)
        """
        if self.model_embeddings is None or cosine_similarity is None:
            raise RuntimeError(
                "No se puede calcular similitud semántica: "
                "asegúrate de tener instalados 'sentence-transformers' "
                "y 'scikit-learn'."
            )

        if len(textos) < 2:
            raise ValueError("Se necesitan al menos 2 textos para calcular similitud.")

        embeddings = self.model_embeddings.encode(textos)
        sim_matrix = cosine_similarity(embeddings)

        if pd is not None:
            etiquetas = [f"Texto {i+1}" for i in range(len(textos))]
            return pd.DataFrame(sim_matrix, columns=etiquetas, index=etiquetas)
        else:
            return sim_matrix.tolist()

    # ---------- SENTIMIENTO (OPCIONAL, PESADO) ----------
    def analizar_sentimiento(
        self,
        texto: str,
        modelo: str = "nlptown/bert-base-multilingual-uncased-sentiment",
    ) -> Dict[str, object]:
        """
        Analiza sentimiento usando transformers (modelo multilingüe).

        ⚠ OJO: esto descarga modelos grandes la primera vez.
        Úsalo solo si realmente lo necesitas.
        """
        if pipeline is None:
            return {
                "sentimiento": "ERROR",
                "score": 0.0,
                "error": "transformers no está instalado (pip install transformers)",
            }

        try:
            clf = pipeline("sentiment-analysis", model=modelo, tokenizer=modelo)
            out = clf(texto)[0]
            return {"sentimiento": out["label"], "score": float(out["score"])}
        except Exception as e:
            return {"sentimiento": "ERROR", "score": 0.0, "error": str(e)}

    # ---------- UTILIDADES VARIAS ----------
    def extraer_nombres_propios(self, texto: str) -> List[str]:
        """Extrae nombres propios (PROPN) del texto."""
        self._check_spacy()
        doc = self.nlp(texto)

        nombres: List[str] = [
            token.text.strip()
            for token in doc
            if token.pos_ == "PROPN" and len(token.text.strip()) > 2
        ]
        # quitar duplicados
        return list(dict.fromkeys(nombres))

    def contar_palabras(self, texto: str, unicas: bool = False) -> int:
        """Cuenta palabras (todas o solo únicas, excluyendo stopwords)."""
        self._check_spacy()
        doc = self.nlp(texto)
        palabras = [
            token.text.lower()
            for token in doc
            if not token.is_punct and not token.is_space and not token.is_stop
        ]
        return len(set(palabras)) if unicas else len(palabras)

    def close(self) -> None:
        """No hace nada especial, pero dejamos el hook por si acaso."""
        # Modelos se liberan automáticamente al finalizar el proceso.
        pass


# ----------------------------------------------------------------------
# Bloque de prueba rápida (opcional)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    print("[PLN] Prueba rápida de carga de modelos...")
    pln = PLN(cargar_modelos=True)

    texto_demo = (
        "El Ministerio de Minas y Energía expidió la Resolución 123 de 2024 "
        "para regular el uso de hidrocarburos en el territorio nacional."
    )

    if pln.nlp is not None:
        print("\nEntidades detectadas:")
        print(pln.extraer_entidades(texto_demo))

        print("\nTemas principales:")
        print(pln.extraer_temas(texto_demo, top_n=5))

        print("\nResumen:")
        print(pln.generar_resumen(texto_demo, num_oraciones=1))
    else:
        print("No se cargó spaCy; instala el modelo de idioma antes de usar PLN.")
