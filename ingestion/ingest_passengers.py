"""
À compléter : ingestion de l'entité `passengers`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import check_columns, fetch_csv

# Colonnes attendues dans chacun des deux systèmes sources (schémas volontairement différents).
EN_COLS = ["passenger_id", "first_name", "last_name", "gender", "nationality", "email",
           "birth_date", "signup_date"]
FR_COLS = ["id_passager", "prenom", "nom", "genre", "nationalite", "email",
           "date_naissance", "date_inscription"]


def _snapshot_files(day: date = None, init: bool = False):
    """Renvoie (subdir, fichier EN, fichier FR) pour le jour demandé (ou init/)."""
    if init:
        return "init", "passengers_en.csv", "passengers_fr.csv"
    assert day is not None
    return (day.strftime("%Y-%m"), f"passengers_en_{day.isoformat()}.csv",
            f"passengers_fr_{day.isoformat()}.csv")


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie les deux snapshots (EN et FR) tels quels dans bronze/, sans les fusionner :
    la consolidation des deux schémas est le travail du silver, pas du bronze."""
    subdir, file_en, file_fr = _snapshot_files(day, init)
    check_columns(fetch_csv(subdir, file_en), EN_COLS, file_en)
    check_columns(fetch_csv(subdir, file_fr), FR_COLS, file_fr)


def create_silver_table(con):
    # TODO : créer silver_passengers avec un schéma de table UNIQUE
    # + is_active + deleted_date + insert_timestamp/update_timestamp (cf. ingest_airports.py).
    raise NotImplementedError


def ingest_silver(day: date = None, init: bool = False):
    # TODO : chargement de la données dans dans silver_passengers par passenger_id
    raise NotImplementedError


def ingest_gold():
    # TODO : reconstruire la/les table(s) de gold avec les données passengers à partir de silver_passengers
    raise NotImplementedError


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Passagers (init) ingérés.")


if __name__ == "__main__":
    init()
