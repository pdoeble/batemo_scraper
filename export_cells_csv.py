import csv
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
SQLITE_PATH = BASE_DIR / "data" / "batemo_cells.sqlite"
OUTPUT_CSV = BASE_DIR / "data" / "batemo_cells.csv"


def open_sqlite(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"SQLite-DB nicht gefunden: {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def export_cells_to_csv(sqlite_path: Path, csv_path: Path) -> None:
    """
    Exportiert die technisch relevanten Parameter der Zellen in eine CSV-Datei.

    Es werden bewusst KEINE IDs, URLs, Timestamps oder Roh-HTML exportiert.
    Nur Name + technische Parameter.
    """
    conn = open_sqlite(sqlite_path)
    cur = conn.cursor()

    # Spalten, die wir für den Export haben wollen
    columns = [
        "name",
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
        # bewusst KEIN release_date (zeitlich)

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
    ]

    col_list = ", ".join(columns)

    cur.execute(f"SELECT {col_list} FROM cells")
    rows = cur.fetchall()

    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        # Header schreiben
        writer.writerow(columns)

        for row in rows:
            writer.writerow([row[col] for col in columns])

    conn.close()

    print(f"[INFO] {len(rows)} Zellen nach '{csv_path}' exportiert.")


def main() -> None:
    export_cells_to_csv(SQLITE_PATH, OUTPUT_CSV)


if __name__ == "__main__":
    main()
