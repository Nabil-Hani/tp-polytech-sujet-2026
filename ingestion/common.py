"""
Utilitaires partagés par les scripts d'ingestion. `get_connection` est fourni tel quel ;
`fetch_csv` est à vous d'implémenter (cf. TODO)
"""
import os
import urllib.request

import duckdb
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")
DB_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")
RAW_BASE = "https://raw.githubusercontent.com/kevinl75/tp-polytech-dataset/main"


def fetch_csv(subdir: str, filename: str) -> pd.DataFrame:
    """Renvoie le contenu de <subdir>/<filename> sous forme de DataFrame pandas.

    Le fichier est d'abord rapatrié tel quel dans bronze/<subdir>/<filename> s'il n'y est pas
    déjà, puis toujours relu depuis le disque : bronze/ est la seule source de vérité locale,
    la source distante n'est contactée qu'une fois par fichier.
    """
    local_path = os.path.join(BRONZE_DIR, subdir, filename)

    if not os.path.exists(local_path):
        url = f"{RAW_BASE}/{subdir}/{filename}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        # Copie octet par octet (pas de pd.read_csv -> to_csv) : le bronze est le fichier brut,
        # sans aucune réinterprétation (formats de date, zéros en tête, séparateurs...).
        with urllib.request.urlopen(url, timeout=30) as response:
            content = response.read()
        if not content:
            raise ValueError(f"Fichier source vide : {url}")
        # Écriture dans un .tmp puis renommage atomique : si le script plante en plein
        # téléchargement, aucun fichier partiel ne reste dans bronze/ (il serait sinon pris
        # pour un fichier complet au prochain passage, puisqu'on ne retélécharge pas).
        tmp_path = local_path + ".tmp"
        with open(tmp_path, "wb") as f:
            f.write(content)
        os.replace(tmp_path, local_path)

    # dtype=str : on relit le brut sans laisser pandas deviner les types (ex. dates FR au format
    # DD/MM/YYYY). Le typage est une responsabilité du silver, fait explicitement en SQL.
    return pd.read_csv(local_path, dtype=str)


def check_columns(df: pd.DataFrame, expected: list, filename: str) -> None:
    """Échoue bruyamment si le fichier reçu n'a pas le schéma attendu (schema drift)."""
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"{filename} : colonnes manquantes {missing} (reçu : {list(df.columns)})")


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)
