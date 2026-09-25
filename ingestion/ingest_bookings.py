"""
À compléter : ingestion de l'entité `bookings`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import check_columns, fetch_csv, get_connection

BOOKING_COLS = [
    "passenger_id", "flight_id", "airport_id", "seat_class", "amount", "currency",
    "booking_date", "booking_channel",
]


def _daily_file(day: date = None, init: bool = False):
    if init:
        return "init", "bookings.csv"
    assert day is not None
    return day.strftime("%Y-%m"), f"bookings_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie le fichier des réservations du jour (ou de init/) tel quel dans bronze/.
    Contrairement aux 3 autres entités, ce n'est pas un snapshot complet mais uniquement les
    réservations effectuées ce jour-là."""
    subdir, filename = _daily_file(day, init)
    df = fetch_csv(subdir, filename)
    check_columns(df, ["booking_id"] + BOOKING_COLS, filename)


def create_silver_table(con):
    # Pas de is_active / deleted_date ici : une réservation est un évènement immuable, elle
    # ne disparaît pas et ne change pas. Seules les colonnes techniques de traçabilité restent.
    # amount en DECIMAL (et non DOUBLE) : un montant financier doit rester exact au centime.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_bookings (
            booking_id VARCHAR PRIMARY KEY,
            passenger_id VARCHAR,
            flight_id VARCHAR,
            airport_id VARCHAR,
            seat_class VARCHAR,
            amount DECIMAL(10, 2),
            currency VARCHAR,
            booking_date DATE,
            booking_channel VARCHAR,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def _check_references(con):
    """Contrôle d'intégrité des réservations du jour (table temporaire `daily`) : chaque
    passager, vol et aéroport référencé doit exister en silver, et airport_id doit être
    l'aéroport de départ du vol. N'est fait que si les silver des dimensions existent déjà
    (cas normal de run_month.py : silver des 4 entités dans l'ordre, bookings en dernier)."""
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    if not {"silver_passengers", "silver_flights", "silver_airports"} <= tables:
        return
    errors = con.execute("""
        SELECT
            count(*) FILTER (WHERE p.passenger_id IS NULL),
            count(*) FILTER (WHERE f.flight_id IS NULL),
            count(*) FILTER (WHERE a.airport_id IS NULL),
            count(*) FILTER (WHERE f.flight_id IS NOT NULL
                             AND d.airport_id <> f.origin_airport_id)
        FROM daily d
        LEFT JOIN silver_passengers p ON p.passenger_id = d.passenger_id
        LEFT JOIN silver_flights f ON f.flight_id = d.flight_id
        LEFT JOIN silver_airports a ON a.airport_id = d.airport_id
    """).fetchone()
    if any(errors):
        raise ValueError(
            "Réservations incohérentes (passager inconnu, vol inconnu, aéroport inconnu, "
            f"aéroport différent du départ du vol) : {errors}")


def ingest_silver(day: date = None, init: bool = False):
    """Relit le fichier du jour depuis bronze/ et insère ses réservations dans silver_bookings."""
    subdir, filename = _daily_file(day, init)
    df = fetch_csv(subdir, filename)  # déjà en cache local : pas de nouveau téléchargement

    con = get_connection()
    create_silver_table(con)
    con.register("raw_daily", df)

    # Typage explicite du brut (tout en VARCHAR depuis le bronze).
    con.execute("""
        CREATE OR REPLACE TEMP TABLE daily AS
        SELECT
            booking_id, passenger_id, flight_id, airport_id, seat_class,
            CAST(amount AS DECIMAL(10, 2)) AS amount,
            currency,
            CAST(booking_date AS DATE) AS booking_date,
            booking_channel
        FROM raw_daily
    """)
    _check_references(con)

    # Insert simple : les réservations sont des faits immuables et le fichier du jour ne
    # contient que les nouveautés, il n'y a donc rien à mettre à jour ni à désactiver.
    # ON CONFLICT DO NOTHING rend l'insert idempotent : rejouer un jour déjà chargé n'ajoute
    # aucun doublon (la clé booking_id existe déjà) et ne touche pas aux lignes existantes,
    # dont insert_timestamp reste donc celui du premier chargement.
    con.execute("""
        INSERT INTO silver_bookings
        SELECT *, now() AS insert_timestamp, now() AS update_timestamp
        FROM daily
        ON CONFLICT (booking_id) DO NOTHING
    """)

    con.execute("DROP TABLE daily")
    con.unregister("raw_daily")
    con.close()


def ingest_gold():
    # TODO : reconstruire la/les table(s) de gold avec les données booking à partir de silver_bookings. 
    raise NotImplementedError


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Réservations (init) ingérés.")


if __name__ == "__main__":
    init()
