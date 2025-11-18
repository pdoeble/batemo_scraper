import argparse
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import DictCursor

# -----------------------------------------------------------
# Pfade
# -----------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
SQLITE_PATH = BASE_DIR / "data" / "batemo_cells.sqlite"

# -----------------------------------------------------------
# Postgres-Verbindung (HIER DEINE DATEN EINTRAGEN ODER PER ENV-VARS)
# -----------------------------------------------------------

PG_HOST = "simdb-dev-db.amg.cloud.corpintra.net"
PG_PORT = 5432
PG_DBNAME = "ThermoExpress"
PG_USER = "pdoeble"
PG_PASSWORD = "@vZC75Q5hDcC"


# -----------------------------------------------------------
# SQLite-Helfer
# -----------------------------------------------------------

def open_sqlite(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"SQLite-DB nicht gefunden: {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def load_cells(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("SELECT * FROM cells")
    return cur.fetchall()


def load_scrape_runs(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("SELECT * FROM scrape_runs")
    return cur.fetchall()


def load_scrape_log(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("SELECT * FROM scrape_log")
    return cur.fetchall()


# -----------------------------------------------------------
# Postgres-Helfer
# -----------------------------------------------------------

def open_postgres() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    conn.autocommit = False
    return conn


def ensure_schema_and_tables(conn_pg, mode: str) -> None:
    """
    Legt Schema batemo + Tabellen an.
    Bei mode='recreate' wird das Schema vorher komplett gedroppt.
    """
    with conn_pg.cursor() as cur:
        if mode == "recreate":
            print("[INFO] Dropping schema batemo (falls vorhanden)...")
            cur.execute("DROP SCHEMA IF EXISTS batemo CASCADE;")

        print("[INFO] Creating schema batemo (falls nicht vorhanden)...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS batemo;")

        # Tabelle cells
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS batemo.cells (
                id BIGSERIAL PRIMARY KEY,
                slug TEXT NOT NULL UNIQUE,
                name TEXT,
                detail_url TEXT NOT NULL,

                cell_origin TEXT,
                cell_format TEXT,
                dimensions_raw TEXT,
                diameter_mm DOUBLE PRECISION,
                height_mm DOUBLE PRECISION,
                weight_g DOUBLE PRECISION,

                nominal_capacity_Ah DOUBLE PRECISION,
                c10_capacity_Ah DOUBLE PRECISION,
                c10_energy_Wh DOUBLE PRECISION,
                continuous_current_A DOUBLE PRECISION,
                peak_current_A DOUBLE PRECISION,
                continuous_power_W DOUBLE PRECISION,
                peak_power_W DOUBLE PRECISION,

                energy_density_Wh_per_kg DOUBLE PRECISION,
                energy_density_Wh_per_l DOUBLE PRECISION,
                power_density_kW_per_kg DOUBLE PRECISION,
                power_density_kW_per_l DOUBLE PRECISION,

                cell_model_version TEXT,
                cell_model_release_date TEXT,
                soc_min_pct DOUBLE PRECISION,
                soc_max_pct DOUBLE PRECISION,
                current_discharge_min_A DOUBLE PRECISION,
                current_charge_max_A DOUBLE PRECISION,
                current_c_min DOUBLE PRECISION,
                current_c_max DOUBLE PRECISION,
                voltage_min_V DOUBLE PRECISION,
                voltage_max_V DOUBLE PRECISION,
                temp_min_C DOUBLE PRECISION,
                temp_max_C DOUBLE PRECISION,

                mean_voltage_c10_V DOUBLE PRECISION,
                mean_voltage_peak_V DOUBLE PRECISION,
                r_eff_mOhm DOUBLE PRECISION,
                c_rate_continuous DOUBLE PRECISION,
                c_rate_peak DOUBLE PRECISION,

                raw_html TEXT
            );
            """
        )

        # Tabelle scrape_runs
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS batemo.scrape_runs (
                id BIGSERIAL PRIMARY KEY,
                started_at TIMESTAMPTZ NOT NULL,
                finished_at TIMESTAMPTZ,
                source_file TEXT,
                total_urls INTEGER,
                success_count INTEGER,
                http_error_count INTEGER,
                parse_error_count INTEGER,
                other_error_count INTEGER,
                duration_sec DOUBLE PRECISION
            );
            """
        )

        # Tabelle scrape_log
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS batemo.scrape_log (
                id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL REFERENCES batemo.scrape_runs(id) ON DELETE CASCADE,
                url TEXT NOT NULL,
                slug TEXT,
                status TEXT NOT NULL,
                http_status INTEGER,
                error_message TEXT,
                scraped_at TIMESTAMPTZ NOT NULL
            );
            """
        )

        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_cells_slug ON batemo.cells(slug);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_scrape_log_run_id ON batemo.scrape_log(run_id);"
        )

    conn_pg.commit()


# -----------------------------------------------------------
# Upload-Logik
# -----------------------------------------------------------

def upsert_cells(conn_pg, cells_rows: List[sqlite3.Row]) -> None:
    """
    Upsert in batemo.cells anhand slug.
    """
    cols = [
        "slug",
        "name",
        "detail_url",
        "cell_origin",
        "cell_format",
        "dimensions_raw",
        "diameter_mm",
        "height_mm",
        "weight_g",
        "nominal_capacity_Ah",
        "c10_capacity_Ah",
        "c10_energy_Wh",
        "continuous_current_A",
        "peak_current_A",
        "continuous_power_W",
        "peak_power_W",
        "energy_density_Wh_per_kg",
        "energy_density_Wh_per_l",
        "power_density_kW_per_kg",
        "power_density_kW_per_l",
        "cell_model_version",
        "cell_model_release_date",
        "soc_min_pct",
        "soc_max_pct",
        "current_discharge_min_A",
        "current_charge_max_A",
        "current_c_min",
        "current_c_max",
        "voltage_min_V",
        "voltage_max_V",
        "temp_min_C",
        "temp_max_C",
        "mean_voltage_c10_V",
        "mean_voltage_peak_V",
        "r_eff_mOhm",
        "c_rate_continuous",
        "c_rate_peak",
        "raw_html",
    ]

    placeholders = ", ".join(["%s"] * len(cols))
    set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols[1:])  # slug bleibt key

    sql = f"""
        INSERT INTO batemo.cells ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (slug) DO UPDATE SET
        {set_clause};
    """

    with conn_pg.cursor() as cur:
        for r in cells_rows:
            values = [r[c] for c in cols]
            cur.execute(sql, values)

    conn_pg.commit()
    print(f"[INFO] {len(cells_rows)} Zellen in batemo.cells upserted.")


def import_runs_and_logs_recreate(
    conn_pg,
    runs_rows: List[sqlite3.Row],
    logs_rows: List[sqlite3.Row],
) -> None:
    """
    Lädt scrape_runs + scrape_log komplett neu in das leere batemo-Schema.
    IDs aus SQLite werden NICHT beibehalten, aber run/log-Verknüpfung bleibt erhalten.
    """
    run_id_map: Dict[int, int] = {}

    with conn_pg.cursor(cursor_factory=DictCursor) as cur:
        # Zuerst alle Runs einfügen und neue IDs merken
        for r in runs_rows:
            old_id = r["id"]
            values = [
                r["started_at"],
                r["finished_at"],
                r["source_file"],
                r["total_urls"],
                r["success_count"],
                r["http_error_count"],
                r["parse_error_count"],
                r["other_error_count"],
                r["duration_sec"],
            ]
            cur.execute(
                """
                INSERT INTO batemo.scrape_runs (
                    started_at, finished_at, source_file,
                    total_urls, success_count,
                    http_error_count, parse_error_count, other_error_count,
                    duration_sec
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id;
                """,
                values,
            )
            new_id = cur.fetchone()["id"]
            run_id_map[old_id] = new_id

        # Dann Logs einfügen mit remappten run_id
        for l in logs_rows:
            old_run_id = l["run_id"]
            new_run_id = run_id_map.get(old_run_id)
            if new_run_id is None:
                # sollte nicht vorkommen, aber sicherheitshalber
                continue

            values_log = [
                new_run_id,
                l["url"],
                l["slug"],
                l["status"],
                l["http_status"],
                l["error_message"],
                l["scraped_at"],
            ]
            cur.execute(
                """
                INSERT INTO batemo.scrape_log (
                    run_id, url, slug, status,
                    http_status, error_message, scraped_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s);
                """,
                values_log,
            )

    conn_pg.commit()
    print(
        f"[INFO] {len(runs_rows)} runs und {len(logs_rows)} log-Einträge in batemo.scrape_runs/_log importiert."
    )


# -----------------------------------------------------------
# main
# -----------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload von batemo_cells.sqlite nach PostgreSQL (Schema batemo)."
    )
    parser.add_argument(
        "--mode",
        choices=["recreate", "upsert"],
        default="upsert",
        help=(
            "recreate: Schema batemo droppen und komplett neu aufbauen; "
            "upsert: nur batemo.cells anhand slug zusammenführen/überschreiben."
        ),
    )
    args = parser.parse_args()

    print(f"[INFO] Verwende SQLite-DB: {SQLITE_PATH}")
    conn_sqlite = open_sqlite(SQLITE_PATH)

    cells_rows = load_cells(conn_sqlite)
    runs_rows = load_scrape_runs(conn_sqlite)
    logs_rows = load_scrape_log(conn_sqlite)

    print(f"[INFO] Zellen: {len(cells_rows)}, runs: {len(runs_rows)}, logs: {len(logs_rows)}")

    print("[INFO] Verbinde zu PostgreSQL...")
    conn_pg = open_postgres()

    try:
        ensure_schema_and_tables(conn_pg, args.mode)

        # Cells immer upserten
        upsert_cells(conn_pg, cells_rows)

        if args.mode == "recreate":
            # Nur im recreate-Modus die Logs aus SQLite komplett neu übernehmen
            import_runs_and_logs_recreate(conn_pg, runs_rows, logs_rows)
        else:
            print("[INFO] mode=upsert: scrape_runs/scrape_log werden NICHT übertragen.")

        conn_pg.commit()
    finally:
        conn_pg.close()
        conn_sqlite.close()

    print("[INFO] Upload fertig.")


if __name__ == "__main__":
    main()
