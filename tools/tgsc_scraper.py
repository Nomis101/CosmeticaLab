#!/usr/bin/env python3
"""
TGSC (The Good Scents Company) unified scraper.
Scrapes substance information and demo formulations, outputs to SQLite.

Usage:
    python3 tgsc_scraper.py discover-substances   # Discover all substance URLs from index pages
    python3 tgsc_scraper.py discover-demos         # Discover which demo pages exist (HEAD requests)
    python3 tgsc_scraper.py scrape-substances      # Scrape all substance pages
    python3 tgsc_scraper.py scrape-demos           # Scrape all demo formulation pages
    python3 tgsc_scraper.py scrape-all             # Scrape both substances and demos
    python3 tgsc_scraper.py test-substance <url>   # Test parsing a single substance page
    python3 tgsc_scraper.py test-demo <url>        # Test parsing a single demo page

    Add --resume to any scrape command to resume an interrupted run.

Output:
    tgsc_perfume_resources.sqlite
"""

import json
import os
import re
import sqlite3
import string
import sys
import tempfile
import time
import traceback
import threading
from pathlib import Path
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://www.thegoodscentscompany.com"
PROJECT_DIR = Path(__file__).parent

# Discovery output files (JSON, used as intermediate state)
SUBSTANCE_URLS_FILE = PROJECT_DIR / "discovered_urls.json"
DEMO_URLS_FILE = PROJECT_DIR / "discovered_demo_urls.json"

# Progress files
SUBSTANCE_PROGRESS_FILE = PROJECT_DIR / "substance_scrape_progress.json"
DEMO_PROGRESS_FILE = PROJECT_DIR / "demo_scrape_progress.json"

# Final output
SQLITE_FILE = PROJECT_DIR / "tgsc_perfume_resources.sqlite"

# Rate limiting
REQUEST_DELAY = 0.3      # seconds between requests per thread
HEAD_DELAY = 0.15         # HEAD requests are lightweight
MAX_RETRIES = 3
RETRY_DELAY = 5           # seconds before retrying
WORKERS = 10              # concurrent threads for substance scraping

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Thread-local sessions for concurrent requests
_thread_local = threading.local()


def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update(HEADERS)
    return _thread_local.session


# Global session for single-threaded phases
SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def fetch(url: str) -> str | None:
    """Fetch a URL with retries and rate limiting (single-threaded)."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 200:
                time.sleep(REQUEST_DELAY)
                return resp.text
            elif resp.status_code == 404:
                return None
            else:
                print(f"  HTTP {resp.status_code} for {url}, retrying...")
                time.sleep(RETRY_DELAY)
        except requests.RequestException as e:
            print(f"  Error fetching {url}: {e}, retrying...")
            time.sleep(RETRY_DELAY)
    print(f"  FAILED after {MAX_RETRIES} attempts: {url}")
    return None


def head_check(url: str) -> bool:
    """Check if a URL exists using a HEAD request."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = SESSION.head(url, timeout=15, allow_redirects=True)
            time.sleep(HEAD_DELAY)
            return resp.status_code == 200
        except requests.RequestException:
            time.sleep(RETRY_DELAY)
    return False


# ---------------------------------------------------------------------------
# Name validation (filters broken/truncated names from scraping artifacts)
# ---------------------------------------------------------------------------

def is_broken_name(name: str) -> bool:
    """Detect truncated chemical names from scraping errors."""
    name = name.strip()
    if not name:
        return True
    # Single character
    if len(name) == 1:
        return True
    # Pure digits (with optional letter suffix like "5a")
    if re.match(r'^\d+[a-z]?$', name):
        return True
    # Truncated fragment: open paren/bracket never closed
    # e.g. "(E", "(Z", "(1S", "(3R", "[(4E", "isoamyl-(2"
    if name.count('(') > name.count(')') or name.count('[') > name.count(']'):
        return True
    # IUPAC fragment starting with "digit-(" e.g. "4-(3", "1-(2"
    if re.match(r'^\d+-?\(', name) and ')' not in name:
        return True
    # Name ends with a trailing hyphen (clearly truncated)
    if name.endswith('-'):
        return True
    return False


def is_truncated_rw(name: str, url: str) -> bool:
    """Detect /data/rw entries with names that look like truncated IUPAC fragments.

    Only flags names that show clear signs of being cut mid-chemical-name,
    NOT legitimate trade names ending in -digit (PEG-8, polysorbate-80, etc.).
    """
    if not url.startswith('/data/rw'):
        return False
    if not re.search(r'-\d+$', name):
        return False
    # Very short names ending in -digit are suspicious (but not 2-letter codes like "WS-3")
    base = re.sub(r'-\d+$', '', name)
    if len(base) <= 1:
        return True
    # IUPAC position-comma numbering (e.g. "2,6-dimethyl-2") indicates truncation
    if re.search(r'\d,\d', name):
        return True
    # Common substituent prefix followed by bare position number
    if re.search(
        r'(methyl|ethyl|propyl|butyl|pentyl|hexyl|heptyl|octyl|nonyl|decyl|'
        r'phenyl|benzyl|vinyl|allyl|acetyl|formyl|oxo|hydroxy|amino|chloro|'
        r'bromo|fluoro|nitro|sulfo|carbonyl|carboxyl)-\d+$', name, re.I
    ):
        return True
    return False


# ---------------------------------------------------------------------------
# SQLite database setup
# ---------------------------------------------------------------------------

def init_db(db_path: Path) -> sqlite3.Connection:
    """Create/open the SQLite database and ensure schema exists."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS substances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            url TEXT,
            cas_number TEXT,
            molecular_weight REAL,
            formula TEXT,
            appearance TEXT,
            melting_point TEXT,
            boiling_point TEXT,
            vapor_pressure TEXT,
            flash_point TEXT,
            logp TEXT,
            shelf_life TEXT,
            storage TEXT,
            soluble_in TEXT,
            insoluble_in TEXT,
            odor_type TEXT,
            odor_strength TEXT,
            substantivity TEXT,
            odor_descriptions TEXT
        );

        CREATE TABLE IF NOT EXISTS demos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            patent TEXT,
            application TEXT,
            notes TEXT,
            source_page TEXT
        );

        CREATE TABLE IF NOT EXISTS demo_ingredients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            demo_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            amount REAL,
            substance_url TEXT,
            substance_id INTEGER,
            FOREIGN KEY (demo_id) REFERENCES demos(id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_substances_url ON substances(url);
        CREATE INDEX IF NOT EXISTS idx_substances_name ON substances(name COLLATE NOCASE);
        CREATE INDEX IF NOT EXISTS idx_substances_cas ON substances(cas_number);
        CREATE INDEX IF NOT EXISTS idx_substances_odor_type ON substances(odor_type);
        CREATE INDEX IF NOT EXISTS idx_demo_ingredients_demo_id ON demo_ingredients(demo_id);
        CREATE INDEX IF NOT EXISTS idx_demo_ingredients_substance_id ON demo_ingredients(substance_id);
        CREATE INDEX IF NOT EXISTS idx_demo_ingredients_substance_url ON demo_ingredients(substance_url);
        CREATE INDEX IF NOT EXISTS idx_demos_name ON demos(name COLLATE NOCASE);
        CREATE INDEX IF NOT EXISTS idx_demo_ingredients_name ON demo_ingredients(name COLLATE NOCASE);
    """)
    conn.commit()
    return conn


def build_fts(conn: sqlite3.Connection):
    """Build full-text search indexes after all data is loaded."""
    print("Building FTS indexes...")

    # Collect alternate names for substances from demo ingredients
    print("  Building alternate name index...")
    alt_names: dict[int, set[str]] = {}
    rows = conn.execute(
        "SELECT DISTINCT substance_id, name FROM demo_ingredients WHERE substance_id IS NOT NULL"
    ).fetchall()
    for sub_id, ing_name in rows:
        if sub_id not in alt_names:
            alt_names[sub_id] = set()
        alt_names[sub_id].add(ing_name.lower())

    # Remove names that match the substance's own name
    sub_names = {}
    for row in conn.execute("SELECT id, name FROM substances"):
        sub_names[row[0]] = row[1].lower()

    for sub_id in list(alt_names.keys()):
        own = sub_names.get(sub_id, "")
        alt_names[sub_id].discard(own)
        if not alt_names[sub_id]:
            del alt_names[sub_id]

    print(f"  {len(alt_names)} substances have alternate names from demos")

    # Substances FTS with real alt_names
    conn.execute("DROP TABLE IF EXISTS substances_fts")
    conn.execute("""
        CREATE VIRTUAL TABLE substances_fts USING fts5(
            name, cas_number, odor_type, formula, alt_names
        )
    """)

    fts_rows = conn.execute(
        "SELECT id, name, cas_number, odor_type, formula FROM substances"
    ).fetchall()
    for sub_id, name, cas, odor, formula in fts_rows:
        alts = " ".join(alt_names.get(sub_id, set()))
        conn.execute(
            "INSERT INTO substances_fts(rowid, name, cas_number, odor_type, formula, alt_names) "
            "VALUES (?,?,?,?,?,?)",
            (sub_id, name, cas or "", odor or "", formula or "", alts if alts else None),
        )

    # Demos FTS
    conn.executescript("""
        DROP TABLE IF EXISTS demos_fts;
        CREATE VIRTUAL TABLE demos_fts USING fts5(
            name, patent, application, notes,
            content='demos',
            content_rowid='id'
        );
        INSERT INTO demos_fts(rowid, name, patent, application, notes)
            SELECT id, name, COALESCE(patent, ''), COALESCE(application, ''),
                   COALESCE(notes, '')
            FROM demos;
    """)

    conn.commit()
    print("FTS indexes built.")


def link_demo_ingredients(conn: sqlite3.Connection):
    """Link demo_ingredients to substances by URL match, then name fallback."""
    print("Linking demo ingredients to substances...")

    # Pass 1: match by URL
    conn.execute("""
        UPDATE demo_ingredients
        SET substance_id = (
            SELECT s.id FROM substances s WHERE s.url = demo_ingredients.substance_url
        )
        WHERE substance_url IS NOT NULL
    """)
    conn.commit()
    linked_url = conn.execute(
        "SELECT COUNT(*) FROM demo_ingredients WHERE substance_id IS NOT NULL"
    ).fetchone()[0]

    # Pass 2: name fallback for unresolved ingredients
    # Build name -> id map (first match wins, case-insensitive)
    name_to_id = {}
    for row in conn.execute("SELECT id, name FROM substances"):
        key = row[1].lower()
        if key not in name_to_id:
            name_to_id[key] = row[0]

    unresolved = conn.execute(
        "SELECT id, name FROM demo_ingredients WHERE substance_id IS NULL"
    ).fetchall()

    updates = []
    for di_id, di_name in unresolved:
        sub_id = name_to_id.get(di_name.lower())
        if sub_id is not None:
            updates.append((sub_id, di_id))

    if updates:
        conn.executemany(
            "UPDATE demo_ingredients SET substance_id = ? WHERE id = ?",
            updates,
        )
        conn.commit()

    linked = conn.execute(
        "SELECT COUNT(*) FROM demo_ingredients WHERE substance_id IS NOT NULL"
    ).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM demo_ingredients").fetchone()[0]
    print(f"Linked {linked}/{total} demo ingredients to substances "
          f"({linked_url} by URL, {linked - linked_url} by name fallback).")


# ---------------------------------------------------------------------------
# Phase 1a: Discover substance URLs
# ---------------------------------------------------------------------------

SINGLE_INDEXES = ["abs-az", "con-az", "fix-az", "ole-az", "peb-az", "tls-az"]
ALPHA_INDEXES = [
    "rawmatex", "essentlx", "extractx", "cosmetix", "resinx",
    "flavorsx", "fragranx", "flavonly", "fragonly", "botaspes",
    "naturocc", "fodadd",
]


def discover_substance_urls():
    """Discover all unique substance URLs from TGSC index pages."""
    all_urls = set()

    for page in SINGLE_INDEXES:
        url = f"{BASE_URL}/{page}.html"
        print(f"Fetching {page}...")
        html = fetch(url)
        if html:
            links = set(re.findall(r"/data/[a-z]{2}\d+\.html", html))
            all_urls.update(links)
            print(f"  Found {len(links)} links")

    for idx in ALPHA_INDEXES:
        for letter in string.ascii_lowercase:
            url = f"{BASE_URL}/{idx}-{letter}.html"
            html = fetch(url)
            if html:
                links = set(re.findall(r"/data/[a-z]{2}\d+\.html", html))
                all_urls.update(links)
                if links:
                    print(f"  {idx}-{letter}: {len(links)} links (total unique: {len(all_urls)})")

    urls_list = sorted(all_urls)
    with open(SUBSTANCE_URLS_FILE, "w") as f:
        json.dump(urls_list, f, indent=2)

    print(f"\nDiscovered {len(urls_list)} unique substance URLs. Saved to {SUBSTANCE_URLS_FILE}")
    return urls_list


# ---------------------------------------------------------------------------
# Phase 1b: Discover demo URLs
# ---------------------------------------------------------------------------

def discover_demo_urls():
    """Use HEAD requests to find which demo pages exist."""
    if not SUBSTANCE_URLS_FILE.exists():
        print("No discovered_urls.json found. Run 'discover-substances' first.")
        sys.exit(1)

    with open(SUBSTANCE_URLS_FILE) as f:
        substance_urls = json.load(f)

    ids = set()
    for u in substance_urls:
        m = re.match(r"/data/[a-z]{2}(\d+)\.html", u)
        if m:
            ids.add(m.group(1))

    ids_sorted = sorted(ids)
    total = len(ids_sorted)
    print(f"Checking {total} IDs for demo pages...")

    existing = []
    already_checked = set()

    resume_file = PROJECT_DIR / "demo_discover_progress.json"
    if resume_file.exists():
        with open(resume_file) as f:
            progress = json.load(f)
        existing = progress.get("existing", [])
        already_checked = set(progress.get("checked", []))
        print(f"Resuming: {len(already_checked)} already checked, {len(existing)} found so far")

    for i, sid in enumerate(ids_sorted):
        if sid in already_checked:
            continue

        url = f"{BASE_URL}/demos/dm{sid}.html"
        exists = head_check(url)
        already_checked.add(sid)

        if exists:
            existing.append(f"/demos/dm{sid}.html")
            print(f"  [{len(already_checked)}/{total}] dm{sid} EXISTS ({len(existing)} found)")
        elif len(already_checked) % 500 == 0:
            print(f"  [{len(already_checked)}/{total}] ... ({len(existing)} found so far)")

        if len(already_checked) % 1000 == 0:
            with open(resume_file, "w") as f:
                json.dump({"existing": existing, "checked": list(already_checked)}, f)

    existing.sort()
    with open(DEMO_URLS_FILE, "w") as f:
        json.dump(existing, f, indent=2)

    if resume_file.exists():
        resume_file.unlink()

    print(f"\nDiscovered {len(existing)} demo pages. Saved to {DEMO_URLS_FILE}")
    return existing


# ---------------------------------------------------------------------------
# Substance page parsing
# ---------------------------------------------------------------------------

# Regex to strip trailing CAS number from title: ", 12345-67-8" at end of string
_CAS_SUFFIX_RE = re.compile(r",\s*\d{1,7}-\d{2}-\d$")


def extract_name_from_title(title: str) -> str:
    """Extract substance name from the page title.

    TGSC titles look like: "substance name, 12345-67-8"
    Naive comma-split breaks names containing commas, e.g. "(Z,E)-phytol, 7541-49-3".
    Instead, we strip the trailing CAS number pattern.
    """
    # Try to remove the trailing ", CAS-number"
    name = _CAS_SUFFIX_RE.sub("", title).strip()
    if not name:
        # Fallback: return the whole title
        name = title.strip()
    return name


def get_label_value(soup, css_class: str, label_pattern: str) -> str | None:
    """Find a <td class=css_class> matching label_pattern and return next sibling td text."""
    for td in soup.find_all("td", class_=css_class):
        text = td.get_text(strip=True)
        if re.match(label_pattern, text, re.I):
            sibling = td.find_next_sibling("td")
            if sibling:
                return sibling.get_text(strip=True) or None
    return None


def get_label_value_all(soup, css_class: str, label_pattern: str) -> list[str]:
    """Like get_label_value but returns ALL matches."""
    results = []
    for td in soup.find_all("td", class_=css_class):
        text = td.get_text(strip=True)
        if re.match(label_pattern, text, re.I):
            sibling = td.find_next_sibling("td")
            if sibling:
                val = sibling.get_text(strip=True)
                if val:
                    results.append(val)
    return results


def get_solubility(soup, label: str) -> list[str]:
    """Extract 'Soluble in' or 'Insoluble in' values."""
    for td in soup.find_all("td", class_="synonyms"):
        text = td.get_text(strip=True)
        if text.lower().startswith(label.lower()):
            row = td.parent
            values = []
            sibling = row.find_next_sibling("tr")
            while sibling:
                tds = sibling.find_all("td")
                if not tds:
                    break
                first_td = tds[0]
                if first_td.get("class") and any(
                    c in ["synonyms", "radw4", "radw8", "qinfr2", "radw5"]
                    for c in first_td.get("class", [])
                ):
                    ft = first_td.get_text(strip=True)
                    if ":" in ft or ft.startswith("Soluble") or ft.startswith("Insoluble"):
                        break
                val = sibling.get_text(strip=True)
                if val and len(val) < 200:
                    values.append(val)
                    sibling = sibling.find_next_sibling("tr")
                else:
                    break
            return values
    return []


def get_odor_descriptions(soup) -> list[str]:
    """Extract all odor descriptions."""
    descriptions = []
    for td in soup.find_all("td", class_="radw5"):
        text = td.get_text(strip=True)
        if text.startswith("Odor Description:"):
            raw = text[len("Odor Description:"):].strip()
            spans = td.find_all("span")
            parts = [s.get_text(strip=True) for s in spans if s.get_text(strip=True)]

            if len(parts) >= 2:
                desc_text = " ".join(parts)
            elif parts:
                desc_text = parts[0]
            else:
                desc_text = raw

            if desc_text:
                descriptions.append(desc_text)
    return descriptions


def parse_substance(html: str, url_path: str) -> dict | None:
    """Parse a single substance page and extract fields."""
    soup = BeautifulSoup(html, "lxml")

    title_tag = soup.find("title")
    if not title_tag or not title_tag.string:
        return None
    title = title_tag.string.strip()
    name = extract_name_from_title(title)

    data = OrderedDict()
    data["name"] = name
    data["url"] = url_path
    data["cas_number"] = get_label_value(soup, "radw8", r"CAS Number:")
    data["molecular_weight"] = get_label_value(soup, "radw8", r"Molecular Weight:")
    data["formula"] = get_label_value(soup, "radw8", r"Formula:")
    data["appearance"] = get_label_value(soup, "radw4", r"Appearance:")

    melting = get_label_value_all(soup, "radw4", r"Melting Point:")
    data["melting_point"] = "; ".join(melting) if melting else None

    boiling = get_label_value_all(soup, "radw4", r"Boiling Point:")
    data["boiling_point"] = "; ".join(boiling) if boiling else None

    data["vapor_pressure"] = get_label_value(soup, "radw4", r"Vapor Pressure:")
    data["flash_point"] = get_label_value(soup, "radw4", r"Flash Point:")
    data["logp"] = get_label_value(soup, "radw4", r"logP \(o/w\):")
    data["shelf_life"] = get_label_value(soup, "radw4", r"Shelf Life:")
    data["storage"] = get_label_value(soup, "radw4", r"Storage:")

    soluble = get_solubility(soup, "Soluble in:")
    data["soluble_in"] = json.dumps(soluble) if soluble else None

    insoluble = get_solubility(soup, "Insoluble in:")
    data["insoluble_in"] = json.dumps(insoluble) if insoluble else None

    # Odor Type
    odor_type = None
    for td in soup.find_all("td", class_="qinfr2"):
        text = td.get_text(strip=True)
        if text.startswith("Odor Type:"):
            odor_type = text[len("Odor Type:"):].strip()
            break
    data["odor_type"] = odor_type or None

    # Odor Strength
    odor_strength = None
    for td in soup.find_all("td", class_="radw5"):
        text = td.get_text(strip=True)
        if text.startswith("Odor Strength:"):
            span = td.find("span")
            if span:
                odor_strength = span.get_text(strip=True)
            else:
                odor_strength = text[len("Odor Strength:"):].strip()
            break
    data["odor_strength"] = odor_strength or None

    # Substantivity
    substantivity = None
    for td in soup.find_all("td", class_="radw5"):
        text = td.get_text(strip=True)
        if text.startswith("Substantivity:"):
            span = td.find("span")
            if span:
                substantivity = span.get_text(strip=True)
            else:
                substantivity = text[len("Substantivity:"):].strip()
            break
    data["substantivity"] = substantivity or None

    # Odor Descriptions (stored as JSON array)
    odor_descs = get_odor_descriptions(soup)
    data["odor_descriptions"] = json.dumps(odor_descs) if odor_descs else None

    return data


# ---------------------------------------------------------------------------
# Demo page parsing
# ---------------------------------------------------------------------------

def parse_demo_page(html: str, url_path: str) -> list[dict]:
    """Parse a demo page and extract all formulations."""
    soup = BeautifulSoup(html, "lxml")
    formulations = []

    table = soup.find("table")
    if not table:
        return formulations

    rows = table.find_all("tr")
    current = None

    for row in rows:
        tds = row.find_all("td")
        if not tds:
            continue

        first_td = tds[0]
        cls = first_td.get("class", [])
        if not cls:
            continue

        css_class = cls[0]

        if css_class == "dmow1":
            if current and current["ingredients"]:
                formulations.append(current)
            current = OrderedDict()
            current["name"] = first_td.get_text(strip=True)
            current["patent"] = None
            current["application"] = None
            current["notes"] = None
            current["source_page"] = url_path
            current["ingredients"] = []

        elif css_class == "dmow2" and current is not None:
            text = first_td.get_text(strip=True)
            a_tag = first_td.find("a")
            if a_tag and "patent" in (a_tag.get("href", "") + text).lower():
                current["patent"] = text
            elif current["patent"] is None and ("patent" in text.lower() or "Pat." in text):
                current["patent"] = text
            else:
                if current["notes"]:
                    current["notes"] += " " + text
                else:
                    current["notes"] = text

        elif css_class == "dmow3" and current is not None:
            current["application"] = first_td.get_text(strip=True)

        elif css_class == "dmow8" and current is not None:
            text = first_td.get_text(strip=True)
            if text and text != "For Fragrance Use":
                if current["notes"]:
                    current["notes"] += " " + text
                else:
                    current["notes"] = text

        elif css_class == "dmow5" and current is not None:
            amount_text = first_td.get_text(strip=True)
            try:
                amount = float(amount_text)
            except ValueError:
                continue

            if len(tds) >= 2:
                name_td = tds[1]
                name = name_td.get_text(strip=True)

                if name.lower() == "total":
                    continue

                a_tag = name_td.find("a")
                substance_url = a_tag.get("href") if a_tag else None

                current["ingredients"].append(OrderedDict([
                    ("name", name),
                    ("amount", amount),
                    ("substance_url", substance_url),
                ]))

    if current and current["ingredients"]:
        formulations.append(current)

    return formulations


# ---------------------------------------------------------------------------
# Progress tracking helpers
# ---------------------------------------------------------------------------

def load_progress(path: Path) -> dict:
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {"completed": [], "failed": []}


def save_progress(path: Path, progress: dict):
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(progress, f)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


# ---------------------------------------------------------------------------
# Substance scraping (multi-threaded)
# ---------------------------------------------------------------------------

def fetch_and_parse_substance(url_path: str) -> tuple[str, dict | None, str]:
    """Fetch and parse a single substance page (thread-safe)."""
    session = _get_session()
    full_url = BASE_URL + url_path
    html = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(full_url, timeout=30)
            if resp.status_code == 200:
                html = resp.text
                time.sleep(REQUEST_DELAY)
                break
            elif resp.status_code == 404:
                return (url_path, None, "404")
            else:
                time.sleep(RETRY_DELAY)
        except requests.RequestException:
            time.sleep(RETRY_DELAY)

    if html is None:
        return (url_path, None, "fetch_failed")

    try:
        data = parse_substance(html, url_path)
        if data:
            return (url_path, data, "ok")
        return (url_path, None, "parse_failed")
    except Exception as e:
        return (url_path, None, f"error: {e}")


def scrape_substances(resume: bool = False):
    """Scrape all substance pages and insert into SQLite."""
    if not SUBSTANCE_URLS_FILE.exists():
        print("No discovered URLs found. Run 'discover-substances' first.")
        sys.exit(1)

    with open(SUBSTANCE_URLS_FILE) as f:
        all_urls = json.load(f)

    conn = init_db(SQLITE_FILE)
    try:
        progress = {"completed": set(), "failed": set()}
        if resume:
            progress_data = load_progress(SUBSTANCE_PROGRESS_FILE)
            progress["completed"] = set(progress_data.get("completed", []))
            progress["failed"] = set(progress_data.get("failed", []))
            print(f"Resuming: {len(progress['completed'])} already done")
        else:
            # Clear substances table for fresh run
            conn.execute("DELETE FROM substances")
            conn.commit()

        remaining = [u for u in all_urls if u not in progress["completed"] and u not in progress["failed"]]
        total = len(all_urls)
        done_start = len(progress["completed"])

        print(f"Total URLs: {total}, Remaining: {len(remaining)}, Workers: {WORKERS}")

        save_interval = 500
        completed_count = 0
        batch = []

        def flush_batch():
            nonlocal batch
            if not batch:
                return
            conn.executemany("""
                INSERT OR IGNORE INTO substances (
                    name, url, cas_number, molecular_weight, formula,
                    appearance, melting_point, boiling_point, vapor_pressure,
                    flash_point, logp, shelf_life, storage, soluble_in,
                    insoluble_in, odor_type, odor_strength, substantivity,
                    odor_descriptions
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            conn.commit()
            batch = []

        def data_to_row(data: dict) -> tuple:
            mw = data.get("molecular_weight")
            if mw:
                try:
                    mw = float(mw)
                except (ValueError, TypeError):
                    mw = None
            return (
                data["name"], data["url"], data.get("cas_number"),
                mw, data.get("formula"),
                data.get("appearance"), data.get("melting_point"),
                data.get("boiling_point"), data.get("vapor_pressure"),
                data.get("flash_point"), data.get("logp"),
                data.get("shelf_life"), data.get("storage"),
                data.get("soluble_in"), data.get("insoluble_in"),
                data.get("odor_type"), data.get("odor_strength"),
                data.get("substantivity"), data.get("odor_descriptions"),
            )

        try:
            with ThreadPoolExecutor(max_workers=WORKERS) as executor:
                futures = {executor.submit(fetch_and_parse_substance, url_path): url_path for url_path in remaining}

                for future in as_completed(futures):
                    url_path, data, status = future.result()
                    completed_count += 1
                    idx = done_start + completed_count

                    if status == "ok" and data:
                        name = data.get("name", "")
                        url = data.get("url", "")
                        if is_broken_name(name) or is_truncated_rw(name, url):
                            progress["completed"].add(url_path)
                            if completed_count % 50 == 0:
                                print(f"[{idx}/{total}] {url_path} SKIP (broken name: {name[:40]})", flush=True)
                        else:
                            batch.append(data_to_row(data))
                            progress["completed"].add(url_path)
                            if completed_count % 50 == 0 or completed_count <= 5:
                                print(f"[{idx}/{total}] {url_path} OK ({name[:40]})", flush=True)
                    else:
                        progress["failed"].add(url_path)
                        print(f"[{idx}/{total}] {url_path} {status}", flush=True)

                    if completed_count % save_interval == 0:
                        flush_batch()
                        save_progress(SUBSTANCE_PROGRESS_FILE, {
                            "completed": list(progress["completed"]),
                            "failed": list(progress["failed"]),
                        })
                        print(f"  Saved progress ({len(progress['completed'])} done, "
                              f"{len(progress['failed'])} failed)...", flush=True)
        finally:
            flush_batch()
            save_progress(SUBSTANCE_PROGRESS_FILE, {
                "completed": list(progress["completed"]),
                "failed": list(progress["failed"]),
            })

        count = conn.execute("SELECT COUNT(*) FROM substances").fetchone()[0]
        print(f"\nDone! {count} substances in database.")
        print(f"Failed: {len(progress['failed'])}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Demo scraping (single-threaded, sequential)
# ---------------------------------------------------------------------------

def scrape_demos(resume: bool = False):
    """Scrape all demo formulation pages and insert into SQLite."""
    if not DEMO_URLS_FILE.exists():
        print("No discovered demo URLs found. Run 'discover-demos' first.")
        sys.exit(1)

    with open(DEMO_URLS_FILE) as f:
        all_urls = json.load(f)

    conn = init_db(SQLITE_FILE)
    try:
        progress = {"completed": set(), "failed": set()}
        if resume:
            progress_data = load_progress(DEMO_PROGRESS_FILE)
            progress["completed"] = set(progress_data.get("completed", []))
            progress["failed"] = set(progress_data.get("failed", []))
            print(f"Resuming: {len(progress['completed'])} already done")
        else:
            conn.execute("DELETE FROM demo_ingredients")
            conn.execute("DELETE FROM demos")
            conn.commit()

        remaining = [u for u in all_urls if u not in progress["completed"] and u not in progress["failed"]]
        total = len(all_urls)
        done = len(progress["completed"])

        print(f"Total demo URLs: {total}, Remaining: {len(remaining)}")

        save_interval = 50
        total_formulations = 0

        for i, url_path in enumerate(remaining):
            full_url = BASE_URL + url_path
            idx = done + i + 1
            print(f"[{idx}/{total}] {url_path}", end=" ", flush=True)

            html = fetch(full_url)
            if html is None:
                progress["failed"].add(url_path)
                print("SKIP (fetch failed)")
                continue

            try:
                formulations = parse_demo_page(html, url_path)
                if formulations:
                    for form in formulations:
                        cursor = conn.execute(
                            "INSERT INTO demos (name, patent, application, notes, source_page) "
                            "VALUES (?, ?, ?, ?, ?)",
                            (form["name"], form.get("patent"), form.get("application"),
                             form.get("notes"), form["source_page"])
                        )
                        demo_id = cursor.lastrowid

                        for ing in form["ingredients"]:
                            conn.execute(
                                "INSERT INTO demo_ingredients (demo_id, name, amount, substance_url) "
                                "VALUES (?, ?, ?, ?)",
                                (demo_id, ing["name"], ing["amount"], ing.get("substance_url"))
                            )

                    conn.commit()
                    total_formulations += len(formulations)
                    progress["completed"].add(url_path)
                    print(f"OK ({len(formulations)} formulations, total: {total_formulations})")
                else:
                    progress["completed"].add(url_path)
                    print("OK (no formulations)")
            except Exception as e:
                progress["failed"].add(url_path)
                print(f"ERROR: {e}")
                traceback.print_exc()

            if idx % save_interval == 0:
                save_progress(DEMO_PROGRESS_FILE, {
                    "completed": list(progress["completed"]),
                    "failed": list(progress["failed"]),
                })
                print(f"  Saved progress ({total_formulations} formulations)...")

        save_progress(DEMO_PROGRESS_FILE, {
            "completed": list(progress["completed"]),
            "failed": list(progress["failed"]),
        })

        demo_count = conn.execute("SELECT COUNT(*) FROM demos").fetchone()[0]
        ing_count = conn.execute("SELECT COUNT(*) FROM demo_ingredients").fetchone()[0]
        print(f"\nDone! {demo_count} demos, {ing_count} ingredients in database.")
        print(f"Failed pages: {len(progress['failed'])}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Finalize: link ingredients and build FTS
# ---------------------------------------------------------------------------

def finalize():
    """Link demo ingredients to substances and build FTS indexes."""
    conn = init_db(SQLITE_FILE)
    try:
        link_demo_ingredients(conn)
        build_fts(conn)

        # Switch from WAL to DELETE journal mode so the database can be opened
        # read-only without needing to create -wal/-shm sidecar files
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.commit()

        # Print stats
        sub_count = conn.execute("SELECT COUNT(*) FROM substances").fetchone()[0]
        demo_count = conn.execute("SELECT COUNT(*) FROM demos").fetchone()[0]
        ing_count = conn.execute("SELECT COUNT(*) FROM demo_ingredients").fetchone()[0]
        resolved = conn.execute(
            "SELECT COUNT(*) FROM demo_ingredients WHERE substance_id IS NOT NULL"
        ).fetchone()[0]
        print(f"\nDatabase finalized: {SQLITE_FILE}")
        print(f"  Substances: {sub_count}")
        print(f"  Demos: {demo_count}")
        print(f"  Demo ingredients: {ing_count}")
        print(f"  Resolved cross-refs: {resolved}/{ing_count} "
              f"({100 * resolved // max(ing_count, 1)}%)")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]
    resume = "--resume" in sys.argv

    if command == "discover-substances":
        discover_substance_urls()

    elif command == "discover-demos":
        discover_demo_urls()

    elif command == "scrape-substances":
        scrape_substances(resume=resume)

    elif command == "scrape-demos":
        scrape_demos(resume=resume)

    elif command == "scrape-all":
        scrape_substances(resume=resume)
        scrape_demos(resume=resume)
        finalize()

    elif command == "finalize":
        finalize()

    elif command == "test-substance":
        url = sys.argv[2] if len(sys.argv) > 2 else "/data/rw1015272.html"
        full_url = BASE_URL + url
        print(f"Testing parse of {full_url}...")
        html = fetch(full_url)
        if html:
            data = parse_substance(html, url)
            print(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            print("Failed to fetch page")

    elif command == "test-demo":
        url = sys.argv[2] if len(sys.argv) > 2 else "/demos/dm1015272.html"
        full_url = BASE_URL + url
        print(f"Testing parse of {full_url}...")
        html = fetch(full_url)
        if html:
            formulations = parse_demo_page(html, url)
            print(f"\nFound {len(formulations)} formulations\n")
            for form in formulations[:3]:
                print(json.dumps(form, indent=2, ensure_ascii=False))
                print("---")
            if len(formulations) > 3:
                print(f"... and {len(formulations) - 3} more")
        else:
            print("Failed to fetch page")

    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
