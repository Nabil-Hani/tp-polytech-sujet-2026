"""
À compléter : ingestion de l'entité `flights`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import check_columns, fetch_csv, get_connection

FLIGHT_COLS = [
    "flight_number", "airline", "origin_airport_id", "destination_airport_id",
    "flight_date", "departure_time", "arrival_time", "aircraft_type",
]


def _snapshot_file(day: date = None, init: bool = False):
    if init:
        return "init", "flights.csv"
    assert day is not None
    return day.strftime("%Y-%m"), f"flights_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie le snapshot complet des vols du jour (ou de init/) tel quel dans bronze/."""
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)
    check_columns(df, ["flight_id"] + FLIGHT_COLS, filename)


def create_silver_table(con):
    # Mêmes colonnes techniques que silver_airports (is_active, deleted_date, insert_timestamp,
    # update_timestamp). Contrairement au bronze (tout en texte), les colonnes métier sont ici
    # typées explicitement : flight_date en DATE, horaires en TIME.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_flights (
            flight_id VARCHAR PRIMARY KEY,
            flight_number VARCHAR,
            airline VARCHAR,
            origin_airport_id VARCHAR,
            destination_airport_id VARCHAR,
            flight_date DATE,
            departure_time TIME,
            arrival_time TIME,
            aircraft_type VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit le snapshot complet depuis bronze/ et l'upsert dans silver_flights (par flight_id),
    puis désactive les vols absents du snapshot."""
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)  # déjà en cache local : pas de nouveau téléchargement
    snapshot_date = date(2025, 8, 31) if init else day

    # Garde-fou : un snapshot vide (fichier réduit à son en-tête) désactiverait TOUS les vols
    # à l'étape de désactivation ci-dessous. On préfère échouer bruyamment.
    if df.empty:
        raise ValueError(f"{filename} : snapshot vide, chargement annulé")

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot", df)

    # Typage explicite du brut (tout en VARCHAR depuis le bronze). Un format inattendu fait
    # échouer la conversion, et donc le chargement, au lieu de charger une valeur fausse.
    typed = """
        SELECT
            flight_id, flight_number, airline, origin_airport_id, destination_airport_id,
            CAST(flight_date AS DATE) AS flight_date,
            CAST(departure_time AS TIME) AS departure_time,
            CAST(arrival_time AS TIME) AS arrival_time,
            aircraft_type
        FROM snapshot
    """

    # Upsert, même principe que silver_airports, avec une différence : la mise à jour n'a lieu
    # que si au moins une valeur a réellement changé (ou si le vol était désactivé et réapparaît).
    # C'est la définition de l'upsert du cours ("mise à jour si la clé existe déjà avec des
    # valeurs différentes") et ça donne un sens à update_timestamp : il indique la date du
    # dernier vrai changement, et non simplement la date du dernier passage du pipeline.
    # IS DISTINCT FROM (et non <>) : comparaison correcte même si une valeur est NULL.
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in FLIGHT_COLS)
    set_clause += ", is_active = true, deleted_date = NULL, update_timestamp = now()"
    changed = " OR ".join(f"silver_flights.{c} IS DISTINCT FROM excluded.{c}" for c in FLIGHT_COLS)
    changed += " OR silver_flights.is_active = false"

    con.execute(f"""
        INSERT INTO silver_flights (
            flight_id, {", ".join(FLIGHT_COLS)},
            is_active, deleted_date, insert_timestamp, update_timestamp
        )
        SELECT flight_id, {", ".join(FLIGHT_COLS)}, true, NULL, now(), now()
        FROM ({typed})
        ON CONFLICT (flight_id) DO UPDATE SET {set_clause}
        WHERE {changed}
    """)

    # Les vols absents du snapshot du jour ont été supprimés dans la source : on les désactive
    # sans jamais les supprimer physiquement (des réservations passées les référencent).
    con.execute(
        """
        UPDATE silver_flights SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true AND flight_id NOT IN (SELECT flight_id FROM snapshot)
        """,
        [snapshot_date],
    )

    con.unregister("snapshot")
    con.close()


def ingest_gold():
    # TODO : reconstruire la/les table(s) de gold avec les données flights à partir de silver_flights
    raise NotImplementedError


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Vols (init) ingérés.")


if __name__ == "__main__":
    init()
