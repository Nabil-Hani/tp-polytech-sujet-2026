"""
À compléter : ingestion de l'entité `passengers`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import check_columns, fetch_csv, get_connection

# Colonnes attendues dans chacun des deux systèmes sources (schémas volontairement différents).
EN_COLS = ["passenger_id", "first_name", "last_name", "gender", "nationality", "email",
           "birth_date", "signup_date"]
FR_COLS = ["id_passager", "prenom", "nom", "genre", "nationalite", "email",
           "date_naissance", "date_inscription"]

# Schéma UNIQUE du silver (hors clé et colonnes techniques). Convention retenue : noms de
# colonnes en anglais (ceux du système EN), valeurs de genre en anglais, dates au format DATE.
# source_system garde la trace du CRM d'origine (lignage), utile pour déboguer une valeur.
PASSENGER_COLS = ["first_name", "last_name", "gender", "nationality", "email",
                  "birth_date", "signup_date", "source_system"]

# Harmonisation des valeurs catégorielles : chaque source code le genre à sa façon.
GENDER_MAP = {"Male": "Male", "Female": "Female", "Homme": "Male", "Femme": "Female"}


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
    # Une seule table pour les deux sources, avec les colonnes techniques habituelles.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_passengers (
            passenger_id VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            nationality VARCHAR,
            email VARCHAR,
            birth_date DATE,
            signup_date DATE,
            source_system VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit les deux snapshots (EN et FR) depuis bronze/, les consolide dans un schéma unique,
    puis les upsert dans silver_passengers (par passenger_id) et désactive les absents."""
    subdir, file_en, file_fr = _snapshot_files(day, init)
    df_en = fetch_csv(subdir, file_en)  # déjà en cache local : pas de nouveau téléchargement
    df_fr = fetch_csv(subdir, file_fr)
    snapshot_date = date(2025, 8, 31) if init else day

    # Garde-fou : un snapshot vide désactiverait tous les passagers de cette source.
    for df, filename in [(df_en, file_en), (df_fr, file_fr)]:
        if df.empty:
            raise ValueError(f"{filename} : snapshot vide, chargement annulé")

    # Contrôles de consolidation, en échec bruyant plutôt qu'en chargement silencieux :
    # - une valeur de genre inconnue n'a pas de correspondance dans GENDER_MAP ;
    # - un même id dans les deux sources serait ambigu (quelle version garder ?). La source
    #   garantit que ça n'arrive pas, on le vérifie quand même.
    unknown = (set(df_en["gender"]) | set(df_fr["genre"])) - set(GENDER_MAP)
    if unknown:
        raise ValueError(f"Valeurs de genre inconnues : {unknown}")
    overlap = set(df_en["passenger_id"]) & set(df_fr["id_passager"])
    if overlap:
        raise ValueError(f"passenger_id présents dans les deux sources : {sorted(overlap)[:10]}")

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot_en", df_en)
    con.register("snapshot_fr", df_fr)

    # Consolidation : chaque source est ramenée au schéma unique (renommage des colonnes,
    # conversion des dates selon le format propre à chaque source, genre harmonisé), puis les
    # deux sont empilées. strptime échoue si une date n'a pas le format attendu.
    gender_case = "CASE {col} " + " ".join(
        f"WHEN '{src}' THEN '{dst}'" for src, dst in GENDER_MAP.items()) + " END"
    consolidated = f"""
        SELECT
            passenger_id, first_name, last_name,
            {gender_case.format(col="gender")} AS gender,
            nationality, email,
            CAST(strptime(birth_date, '%Y-%m-%d') AS DATE) AS birth_date,
            CAST(strptime(signup_date, '%Y-%m-%d') AS DATE) AS signup_date,
            'EN' AS source_system
        FROM snapshot_en
        UNION ALL
        SELECT
            id_passager, prenom, nom,
            {gender_case.format(col="genre")},
            nationalite, email,
            CAST(strptime(date_naissance, '%d/%m/%Y') AS DATE),
            CAST(strptime(date_inscription, '%d/%m/%Y') AS DATE),
            'FR'
        FROM snapshot_fr
    """
    con.execute(f"CREATE OR REPLACE TEMP TABLE snapshot AS {consolidated}")

    # Upsert conditionnel, même logique que silver_flights : mise à jour uniquement si une
    # valeur a réellement changé (ex. correction d'email) ou si le passager réapparaît.
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in PASSENGER_COLS)
    set_clause += ", is_active = true, deleted_date = NULL, update_timestamp = now()"
    changed = " OR ".join(
        f"silver_passengers.{c} IS DISTINCT FROM excluded.{c}" for c in PASSENGER_COLS)
    changed += " OR silver_passengers.is_active = false"

    con.execute(f"""
        INSERT INTO silver_passengers (
            passenger_id, {", ".join(PASSENGER_COLS)},
            is_active, deleted_date, insert_timestamp, update_timestamp
        )
        SELECT passenger_id, {", ".join(PASSENGER_COLS)}, true, NULL, now(), now()
        FROM snapshot
        ON CONFLICT (passenger_id) DO UPDATE SET {set_clause}
        WHERE {changed}
    """)

    # Désactivation des passagers absents des DEUX snapshots consolidés (et non d'un seul :
    # un passager FR est forcément absent du fichier EN, ça ne veut pas dire qu'il a disparu).
    con.execute(
        """
        UPDATE silver_passengers SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true AND passenger_id NOT IN (SELECT passenger_id FROM snapshot)
        """,
        [snapshot_date],
    )

    con.execute("DROP TABLE snapshot")
    con.unregister("snapshot_en")
    con.unregister("snapshot_fr")
    con.close()


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
