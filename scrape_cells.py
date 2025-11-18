import datetime as dt
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


# Pfade
BASE_DIR = Path(__file__).resolve().parent
URLS_PATH = BASE_DIR / "data" / "cell_urls.txt"
DB_PATH = BASE_DIR / "data" / "batemo_cells.sqlite"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BatemoScraper/0.1; +local-use-only)"
}


# ---------------------------------------------------------------------------
# Hilfsfunktionen: Parsing
# ---------------------------------------------------------------------------

def normalize_whitespace(text: str) -> str:
    """Reduziert alle Whitespace-Sequenzen auf einfache Leerzeichen."""
    return re.sub(r"\s+", " ", text).strip()


def to_float(value: Optional[str]) -> Optional[float]:
    """Konvertiert einen String robust in float (Punkt/Komma)."""
    if not value:
        return None
    s = value.strip().replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def extract_label_value(soup: BeautifulSoup, label_prefix: str) -> Optional[str]:
    """
    Sucht nach einem Textknoten, der mit label_prefix beginnt
    (z.B. "Cell Origin", "Cell Format", "Dimen", "Weight")
    und gibt den Rest des Parent-Texts zurück.
    """
    node = soup.find(string=re.compile(rf"^{re.escape(label_prefix)}"))
    if not node:
        return None

    parent = node.parent
    full = parent.get_text(" ", strip=True)
    # z.B. "Cell Origin sourced by Batemo" -> "sourced by Batemo"
    value = full.replace(label_prefix, "", 1).strip()
    return value or None


def extract_block(text: str, start_label: str, next_labels: List[str]) -> Optional[str]:
    """
    Schneidet aus dem normalisierten Text den Block ab start_label
    bis vor das erste Vorkommen eines der next_labels.
    """
    idx_start = text.find(start_label)
    if idx_start == -1:
        return None

    idx_from = idx_start + len(start_label)
    idx_end = len(text)
    for end_label in next_labels:
        j = text.find(end_label, idx_from)
        if j != -1 and j < idx_end:
            idx_end = j

    return text[idx_start:idx_end]


def parse_first_float(pattern_text: Optional[str], pattern: str) -> Optional[float]:
    """
    Sucht den ersten float nach pattern im gegebenen Text
    und gibt ihn als float zurück.
    """
    if not pattern_text:
        return None
    m = re.search(pattern, pattern_text)
    if not m:
        return None
    return to_float(m.group(1))


def parse_range_simple(text: str, label: str, unit: Optional[str] = None) -> Tuple[Optional[float], Optional[float]]:
    """
    Parst einfache Bereiche wie:

    'State of Charge Range 0 … 100%'
    'Voltage Range 2.5 … 4.2 V'
    'Temper­a­ture Range -20 … 80 °C'
    """
    if unit:
        pattern = rf"{re.escape(label)}\s*([-0-9.,]+)\s*[.…]+\s*([-0-9.,]+)\s*{re.escape(unit)}"
    else:
        pattern = rf"{re.escape(label)}\s*([-0-9.,]+)\s*[.…]+\s*([-0-9.,]+)"

    m = re.search(pattern, text)
    if not m:
        return None, None
    return to_float(m.group(1)), to_float(m.group(2))


def parse_current_range(text: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Parst die Zeile z.B.:

    '-90 A discharge … 12 A charge (-30C … 4C)'
    -> (I_dis_min, I_ch_max, C_min, C_max)
    """
    # Ströme
    m_i = re.search(
        r"(-?\d+\.?\d*)\s*A\s*discharge\s*[.…]+\s*(\-?\d+\.?\d*)\s*A\s*charge",
        text,
    )
    i_dis_min = to_float(m_i.group(1)) if m_i else None
    i_ch_max = to_float(m_i.group(2)) if m_i else None

    # C-Rates in Klammern
    m_c = re.search(
        r"\(\s*(-?\d+\.?\d*)\s*C\s*[.…]+\s*(\-?\d+\.?\d*)\s*C\s*\)",
        text,
    )
    c_min = to_float(m_c.group(1)) if m_c else None
    c_max = to_float(m_c.group(2)) if m_c else None

    return i_dis_min, i_ch_max, c_min, c_max


def parse_cell_page(html: str, url: str) -> Dict[str, Any]:
    """
    Parst eine Batemo-Detailseite so weit wie möglich in ein Dict.
    Fehlerhafte/fehlende Felder bleiben None.
    """
    soup = BeautifulSoup(html, "lxml")

    text = soup.get_text(separator="\n", strip=True)
    norm_text = normalize_whitespace(text)

    data: Dict[str, Any] = {}

    # Grundlegende Meta-Infos
    h1 = soup.find("h1")
    if h1:
        data["name"] = h1.get_text(strip=True)
    else:
        data["name"] = None

    parsed_url = urlparse(url)
    slug = parsed_url.path.rstrip("/").split("/")[-1]
    data["slug"] = slug
    data["detail_url"] = url

    # Übersicht: Cell Origin, Format, Dimensionen, Gewicht
    data["cell_origin"] = extract_label_value(soup, "Cell Origin")
    data["cell_format"] = extract_label_value(soup, "Cell Format")

    dim_raw = extract_label_value(soup, "Dimen")
    data["dimensions_raw"] = dim_raw
    diameter_mm: Optional[float] = None
    height_mm: Optional[float] = None
    if dim_raw:
        # Muster: "18.3 x 65 mm" oder "18.3×65 mm"
        m_dim = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*[x×]\s*([0-9]+(?:\.[0-9]+)?)", dim_raw)
        if m_dim:
            diameter_mm = to_float(m_dim.group(1))
            height_mm = to_float(m_dim.group(2))
    data["diameter_mm"] = diameter_mm
    data["height_mm"] = height_mm

    weight_raw = extract_label_value(soup, "Weight")
    weight_g: Optional[float] = None
    if weight_raw:
        m_w = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*g", weight_raw)
        weight_g = to_float(m_w.group(1)) if m_w else to_float(weight_raw)
    data["weight_g"] = weight_g

    # Blöcke Capacity / Current / Energy / Power / Energy Density / Power Density
    cap_block = extract_block(
        norm_text,
        "Capacity",
        [" Current", " Energy", " Power", " Energy Density", " Power Density"],
    )
    current_block = extract_block(
        norm_text,
        "Current",
        [" Energy", " Power", " Energy Density", " Power Density"],
    )
    energy_block = extract_block(
        norm_text,
        "Energy",
        [" Power", " Energy Density", " Power Density"],
    )
    power_block = extract_block(
        norm_text,
        "Power",
        [" Energy Density", " Power Density"],
    )
    energy_density_block = extract_block(
        norm_text,
        "Energy Density",
        [" Power Density", " Batemo Cell Model Version"],
    )
    power_density_block = extract_block(
        norm_text,
        "Power Density",
        [" Batemo Cell Model Version", "##", " Batemo Cell Model"],
    )

    # Capacity
    data["nominal_capacity_Ah"] = parse_first_float(
        cap_block, r"nominal\s*([0-9]+(?:\.[0-9]+)?)\s*Ah"
    )
    data["c10_capacity_Ah"] = parse_first_float(
        cap_block, r"C/10\s*([0-9]+(?:\.[0-9]+)?)\s*Ah"
    )

    # Current
    data["continuous_current_A"] = parse_first_float(
        current_block, r"contin\w*\s*([0-9]+(?:\.[0-9]+)?)\s*A"
    )
    data["peak_current_A"] = parse_first_float(
        current_block, r"peak\s*([0-9]+(?:\.[0-9]+)?)\s*A"
    )

    # Energy
    data["c10_energy_Wh"] = parse_first_float(
        energy_block, r"C/10\s*([0-9]+(?:\.[0-9]+)?)\s*Wh"
    )

    # Power
    data["continuous_power_W"] = parse_first_float(
        power_block, r"contin\w*\s*([0-9]+(?:\.[0-9]+)?)\s*W"
    )
    data["peak_power_W"] = parse_first_float(
        power_block, r"peak\s*([0-9]+(?:\.[0-9]+)?)\s*W"
    )

    # Energy Density
    data["energy_density_Wh_per_kg"] = parse_first_float(
        energy_density_block, r"gravi\w*\s*([0-9]+(?:\.[0-9]+)?)\s*Wh/kg"
    )
    data["energy_density_Wh_per_l"] = parse_first_float(
        energy_density_block, r"volumetric\s*([0-9]+(?:\.[0-9]+)?)\s*Wh/l"
    )

    # Power Density
    data["power_density_kW_per_kg"] = parse_first_float(
        power_density_block, r"gravi\w*\s*([0-9]+(?:\.[0-9]+)?)\s*kW/kg"
    )
    data["power_density_kW_per_l"] = parse_first_float(
        power_density_block, r"volumetric\s*([0-9]+(?:\.[0-9]+)?)\s*kW/l"
    )

    # Model-Infos
    # Version
    m_ver = re.search(r"Batemo Cell Model Version\s*([0-9.]+)", norm_text)
    data["cell_model_version"] = m_ver.group(1) if m_ver else None

    # Release Date -> nach Möglichkeit in ISO-Datum umwandeln
    cell_model_release_date: Optional[str] = None
    m_date = re.search(
        r"Release Date\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})", norm_text
    )
    if m_date:
        raw_date = m_date.group(1)
        try:
            dt_date = dt.datetime.strptime(raw_date, "%B %d, %Y").date()
            cell_model_release_date = dt_date.isoformat()
        except ValueError:
            cell_model_release_date = raw_date
    data["cell_model_release_date"] = cell_model_release_date

    # SoC Range
    soc_min, soc_max = parse_range_simple(norm_text, "State of Charge Range", "%")
    data["soc_min_pct"] = soc_min
    data["soc_max_pct"] = soc_max

    # Current Range (inkl. C-Rates)
    i_dis_min, i_ch_max, c_min, c_max = parse_current_range(norm_text)
    data["current_discharge_min_A"] = i_dis_min
    data["current_charge_max_A"] = i_ch_max
    data["current_c_min"] = c_min
    data["current_c_max"] = c_max

    # Voltage Range
    v_min, v_max = parse_range_simple(norm_text, "Voltage Range", "V")
    data["voltage_min_V"] = v_min
    data["voltage_max_V"] = v_max

    # Temperaturbereich
    t_min, t_max = parse_range_simple(norm_text, "Temper", "°C")
    data["temp_min_C"] = t_min
    data["temp_max_C"] = t_max

    # Abgeleitete Größen
    c10_cap = data.get("c10_capacity_Ah")
    c10_energy = data.get("c10_energy_Wh")
    peak_power = data.get("peak_power_W")
    peak_current = data.get("peak_current_A")
    nominal_cap = data.get("nominal_capacity_Ah")
    cont_current = data.get("continuous_current_A")

    mean_v_c10 = None
    if c10_cap and c10_energy:
        mean_v_c10 = c10_energy / c10_cap
    data["mean_voltage_c10_V"] = mean_v_c10

    mean_v_peak = None
    if peak_power and peak_current:
        mean_v_peak = peak_power / peak_current
    data["mean_voltage_peak_V"] = mean_v_peak

    r_eff_mOhm = None
    if mean_v_c10 and mean_v_peak and peak_current:
        dv = mean_v_c10 - mean_v_peak
        if dv > 0:
            r_eff_mOhm = 1000.0 * dv / peak_current
    data["r_eff_mOhm"] = r_eff_mOhm

    c_rate_cont = None
    if nominal_cap and cont_current:
        c_rate_cont = cont_current / nominal_cap
    data["c_rate_continuous"] = c_rate_cont

    c_rate_peak = None
    if nominal_cap and peak_current:
        c_rate_peak = peak_current / nominal_cap
    data["c_rate_peak"] = c_rate_peak

    # Roh-HTML mit ablegen
    data["raw_html"] = html

    return data


# ---------------------------------------------------------------------------
# DB-Setup
# ---------------------------------------------------------------------------

def init_db(db_path: Path) -> sqlite3.Connection:
    """Initialisiert die SQLite-DB und legt Tabellen an (falls nicht vorhanden)."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS cells (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name TEXT,
            detail_url TEXT NOT NULL,

            cell_origin TEXT,
            cell_format TEXT,
            dimensions_raw TEXT,
            diameter_mm REAL,
            height_mm REAL,
            weight_g REAL,

            nominal_capacity_Ah REAL,
            c10_capacity_Ah REAL,
            c10_energy_Wh REAL,
            continuous_current_A REAL,
            peak_current_A REAL,
            continuous_power_W REAL,
            peak_power_W REAL,

            energy_density_Wh_per_kg REAL,
            energy_density_Wh_per_l REAL,
            power_density_kW_per_kg REAL,
            power_density_kW_per_l REAL,

            cell_model_version TEXT,
            cell_model_release_date TEXT,
            soc_min_pct REAL,
            soc_max_pct REAL,
            current_discharge_min_A REAL,
            current_charge_max_A REAL,
            current_c_min REAL,
            current_c_max REAL,
            voltage_min_V REAL,
            voltage_max_V REAL,
            temp_min_C REAL,
            temp_max_C REAL,

            mean_voltage_c10_V REAL,
            mean_voltage_peak_V REAL,
            r_eff_mOhm REAL,
            c_rate_continuous REAL,
            c_rate_peak REAL,

            raw_html TEXT
        );

        CREATE TABLE IF NOT EXISTS scrape_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            source_file TEXT,
            total_urls INTEGER,
            success_count INTEGER,
            http_error_count INTEGER,
            parse_error_count INTEGER,
            other_error_count INTEGER,
            duration_sec REAL
        );

        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            url TEXT NOT NULL,
            slug TEXT,
            status TEXT NOT NULL,
            http_status INTEGER,
            error_message TEXT,
            scraped_at TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES scrape_runs(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_cells_slug ON cells(slug);
        CREATE INDEX IF NOT EXISTS idx_scrape_log_run_id ON scrape_log(run_id);
        """
    )

    conn.commit()
    return conn


def start_scrape_run(conn: sqlite3.Connection, source_file: str) -> int:
    """Erzeugt einen Eintrag in scrape_runs und gibt run_id zurück."""
    started_at = dt.datetime.now().isoformat(timespec="seconds")
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO scrape_runs (started_at, source_file)
        VALUES (?, ?)
        """,
        (started_at, source_file),
    )
    conn.commit()
    return cur.lastrowid


def finish_scrape_run(
    conn: sqlite3.Connection,
    run_id: int,
    total_urls: int,
    success_count: int,
    http_error_count: int,
    parse_error_count: int,
    other_error_count: int,
    duration_sec: float,
) -> None:
    """Aktualisiert den Eintrag in scrape_runs mit Ergebniszahlen."""
    finished_at = dt.datetime.now().isoformat(timespec="seconds")
    conn.execute(
        """
        UPDATE scrape_runs
        SET finished_at = ?, total_urls = ?, success_count = ?,
            http_error_count = ?, parse_error_count = ?, other_error_count = ?,
            duration_sec = ?
        WHERE id = ?
        """,
        (
            finished_at,
            total_urls,
            success_count,
            http_error_count,
            parse_error_count,
            other_error_count,
            duration_sec,
            run_id,
        ),
    )
    conn.commit()


def log_result(
    conn: sqlite3.Connection,
    run_id: int,
    url: str,
    slug: Optional[str],
    status: str,
    http_status: Optional[int],
    error_message: Optional[str],
) -> None:
    """Trägt eine einzelne URL in scrape_log ein."""
    scraped_at = dt.datetime.now().isoformat(timespec="seconds")
    if error_message and len(error_message) > 500:
        error_message = error_message[:500]

    conn.execute(
        """
        INSERT INTO scrape_log (
            run_id, url, slug, status, http_status, error_message, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, url, slug, status, http_status, error_message, scraped_at),
    )
    conn.commit()


def upsert_cell(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    """
    Schreibt die Zell-Daten in die Tabelle cells.
    Bei gleicher slug wird aktualisiert (UPSERT).
    """
    columns = [
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
    values = [data.get(col) for col in columns]

    placeholders = ", ".join("?" for _ in columns)
    set_clause = ", ".join(f"{col}=excluded.{col}" for col in columns[1:])  # slug bleibt PK

    sql = f"""
        INSERT INTO cells ({", ".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(slug) DO UPDATE SET
            {set_clause}
    """

    conn.execute(sql, values)
    conn.commit()


# ---------------------------------------------------------------------------
# Hauptlogik
# ---------------------------------------------------------------------------

def load_urls(path: Path) -> List[str]:
    """Lädt alle URLs aus der Textdatei (eine pro Zeile)."""
    with path.open("r", encoding="utf-8") as f:
        urls = [
            line.strip()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        ]
    return urls


def scrape_all() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    if not URLS_PATH.exists():
        raise FileNotFoundError(f"URL-Datei nicht gefunden: {URLS_PATH}")

    urls = load_urls(URLS_PATH)

    conn = init_db(DB_PATH)
    run_id = start_scrape_run(conn, str(URLS_PATH))

    total_urls = len(urls)
    success_count = 0
    http_error_count = 0
    parse_error_count = 0
    other_error_count = 0

    t0 = time.time()

    session = requests.Session()
    session.headers.update(HEADERS)

    print(f"[INFO] Starte Scrape-Run {run_id} mit {total_urls} URLs")

    for idx, url in enumerate(urls, start=1):
        print(f"[INFO] ({idx}/{total_urls}) Hole {url}")
        slug = None
        http_status: Optional[int] = None

        try:
            resp = session.get(url, timeout=20)
            http_status = resp.status_code

            if resp.status_code != 200:
                http_error_count += 1
                log_result(
                    conn,
                    run_id,
                    url,
                    slug=None,
                    status="http_error",
                    http_status=http_status,
                    error_message=f"HTTP {resp.status_code}",
                )
                print(f"[WARN] HTTP-Fehler {resp.status_code} bei {url}")
                time.sleep(0.5)
                continue

            html = resp.text
            data = parse_cell_page(html, url)
            slug = data.get("slug")

            if not data.get("name") or not slug:
                # wesentliche Infos fehlen -> als Parse-Fehler zählen
                parse_error_count += 1
                log_result(
                    conn,
                    run_id,
                    url,
                    slug=slug,
                    status="parse_error",
                    http_status=http_status,
                    error_message="Name oder Slug nicht gefunden",
                )
                print(f"[WARN] Parse-Fehler (Name/Slug) bei {url}")
                time.sleep(0.5)
                continue

            upsert_cell(conn, data)
            success_count += 1
            log_result(
                conn,
                run_id,
                url,
                slug=slug,
                status="ok",
                http_status=http_status,
                error_message=None,
            )

        except requests.RequestException as e:
            http_error_count += 1
            log_result(
                conn,
                run_id,
                url,
                slug=slug,
                status="http_error",
                http_status=http_status,
                error_message=str(e),
            )
            print(f"[ERROR] HTTP-Exception bei {url}: {e}")

        except Exception as e:
            other_error_count += 1
            log_result(
                conn,
                run_id,
                url,
                slug=slug,
                status="other_error",
                http_status=http_status,
                error_message=str(e),
            )
            print(f"[ERROR] Unerwarteter Fehler bei {url}: {e}")

        # Kleine Pause, um die Seite nicht zu stressen
        time.sleep(0.5)

    duration_sec = time.time() - t0

    finish_scrape_run(
        conn,
        run_id,
        total_urls=total_urls,
        success_count=success_count,
        http_error_count=http_error_count,
        parse_error_count=parse_error_count,
        other_error_count=other_error_count,
        duration_sec=duration_sec,
    )

    print("\n[INFO] Scrape fertig.")
    print(f"       Gesamt-URLs     : {total_urls}")
    print(f"       Erfolgreich     : {success_count}")
    print(f"       HTTP-Fehler     : {http_error_count}")
    print(f"       Parse-Fehler    : {parse_error_count}")
    print(f"       Sonstige Fehler : {other_error_count}")
    print(f"       Dauer (s)       : {duration_sec:.1f}")
    print(f"[INFO] Ergebnisse in DB: {DB_PATH}")


if __name__ == "__main__":
    scrape_all()
