import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.batemo.com/products/batemo-cell-explorer/"
MODE = "normal"
VIEW = "power-vs-energy-gravimetric"

# Ausgabedatei mit allen gefundenen Detail-URLs (eine pro Zeile)
OUTPUT_PATH = "data/cell_urls.txt"

# Ganz einfacher Header, damit wir nicht wie ein No-Name-Bot aussehen
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BatemoScraper/0.1; +local-use-only)"
}


def build_listing_url(page: int) -> str:
    """
    Baut die URL für die Listing-Seiten des Batemo Cell Explorers.

    Seite 1 hat keinen product-page-Parameter,
    ab Seite 2 verwenden sie ?product-page=2 usw.
    """
    if page <= 1:
        return f"{BASE_URL}?mode={MODE}&view={VIEW}"
    else:
        return f"{BASE_URL}?mode={MODE}&view={VIEW}&product-page={page}"


def extract_cell_urls_from_html(html: str) -> list[str]:
    """
    Extrahiert alle Detail-URLs von Zellen aus einer Listing-Seite.

    Strategie: Suche alle <a>-Tags mit href, deren Pfad
    mit '/products/batemo-cell-explorer/' beginnt.
    """
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href:
            continue

        # Absolut-/Relativ-URL vereinheitlichen
        parsed = urlparse(href)
        path = parsed.path if parsed.netloc else href

        # Nur Produktseiten des Cell Explorers
        if path.startswith("/products/batemo-cell-explorer/"):
            full_url = urljoin(BASE_URL, href)
            urls.append(full_url)

    return urls


def collect_all_cell_urls() -> list[str]:
    """
    Läuft über alle Seiten des Explorers und sammelt die Detail-URLs.

    Abbruchkriterium:
      - Wenn auf einer Seite keine neuen Zellen-URLs mehr gefunden werden,
        brechen wir ab (Ende der Pagination).
    """
    seen: set[str] = set()
    page = 1

    while True:
        url = build_listing_url(page)
        print(f"[INFO] Lade Listing-Seite {page}: {url}")

        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 404:
            print(f"[INFO] Seite {page} liefert 404 -> Ende der Liste.")
            break

        resp.raise_for_status()

        page_urls = extract_cell_urls_from_html(resp.text)

        # Nur neue URLs gegenüber den bisherigen
        new_urls = [u for u in page_urls if u not in seen]

        if not new_urls:
            print(f"[INFO] Keine neuen URLs auf Seite {page} -> Ende.")
            break

        print(f"[INFO] Gefundene neue Zellen auf Seite {page}: {len(new_urls)}")

        for u in new_urls:
            seen.add(u)

        # Kleine Pause, um höflich zu sein
        time.sleep(1.0)

        page += 1

    all_urls = sorted(seen)
    print(f"[INFO] Insgesamt gefundene Zellen: {len(all_urls)}")
    return all_urls


def write_urls_to_file(urls: list[str], path: str) -> None:
    """
    Schreibt alle URLs in eine Textdatei, eine URL pro Zeile.
    """
    with open(path, "w", encoding="utf-8") as f:
        for url in urls:
            f.write(url + "\n")

    print(f"[INFO] URLs nach '{path}' geschrieben.")


def main() -> None:
    urls = collect_all_cell_urls()
    write_urls_to_file(urls, OUTPUT_PATH)


if __name__ == "__main__":
    main()
