"""
CMF Company Registry Reconciliation.

Fetches the current official company lists from CMF and compares them against
the companies already in market.db. Flags:
  - Name changes (same RUT, different name) → possible rebranding after M&A
  - New RUTs on CMF not yet in the DB → new entrants to download
  - RUTs in the DB not on CMF anymore → companies that ceased / were absorbed

Run each quarter BEFORE downloading XBRL files, so you know what to expect.
Usage: python reconcile_cmf.py
"""

import sqlite3
import urllib.request
import re
from pathlib import Path

DB = Path(__file__).parent.parent / "market.db"

CMF_URLS = {
    "general": "https://www.cmfchile.cl/institucional/mercados/consulta.php?mercado=S&Estado=VI&consulta=CSGEN",
    "life":    "https://www.cmfchile.cl/institucional/mercados/consulta.php?mercado=S&Estado=VI&consulta=CSVID",
}


def fetch_cmf_list(url: str) -> dict[str, str]:
    """
    Fetch a CMF company list page and parse RUT → name pairs.
    Returns {rut: name}.
    """
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        html = resp.read().decode("utf-8", errors="replace")

    companies = {}

    # CMF renders company rows as table cells; RUT appears as digits-digit pattern
    # Pattern observed: RUT in one cell, name in adjacent cell
    # Try to find paired (rut, name) from table rows
    rows = re.findall(
        r'(\d{7,8}-[\dKk])\s*</td>\s*<td[^>]*>\s*([^<]{5,120})',
        html,
        re.IGNORECASE,
    )
    for rut_raw, name_raw in rows:
        rut = rut_raw.strip().upper()
        name = re.sub(r'\s+', ' ', name_raw).strip().upper()
        if name:
            companies[rut] = name

    # Fallback: looser pattern if the table structure differs
    if not companies:
        for rut_raw in re.findall(r'\d{7,8}-[\dKk]', html):
            rut = rut_raw.strip().upper()
            idx = html.find(rut_raw)
            snippet = html[idx: idx + 300]
            name_match = re.search(r'([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s\.,]{10,100})', snippet)
            if name_match:
                companies[rut] = name_match.group(1).strip().upper()

    return companies


def load_db_companies(conn: sqlite3.Connection) -> dict[str, str]:
    """Returns {rut: name} for all companies currently in the DB."""
    rows = conn.execute("SELECT rut, UPPER(COALESCE(name,'')) FROM companies").fetchall()
    return {rut: name for rut, name in rows}


def reconcile():
    conn = sqlite3.connect(str(DB))
    db_companies = load_db_companies(conn)
    conn.close()

    print("Fetching CMF company lists...\n")

    cmf_all: dict[str, tuple[str, str]] = {}  # rut → (name, type)
    for company_type, url in CMF_URLS.items():
        try:
            companies = fetch_cmf_list(url)
            print(f"  {company_type}: {len(companies)} companies fetched from CMF")
            for rut, name in companies.items():
                cmf_all[rut] = (name, company_type)
        except Exception as e:
            print(f"  ERROR fetching {company_type} list: {e}")

    print()

    # ── 1. Name changes ────────────────────────────────────────────────────────
    name_changes = []
    for rut, (cmf_name, _) in cmf_all.items():
        db_name = db_companies.get(rut, "").upper().strip()
        if db_name and db_name != cmf_name:
            # Ignore minor punctuation differences
            def normalize(s):
                return re.sub(r'[^A-Z0-9]', '', s.upper())
            if normalize(db_name) != normalize(cmf_name):
                name_changes.append((rut, db_name, cmf_name))

    if name_changes:
        print("!  NAME CHANGES DETECTED (same RUT, different name — possible post-M&A rebranding):")
        for rut, old, new in name_changes:
            print(f"   {rut}")
            print(f"     DB  : {old}")
            print(f"     CMF : {new}")
        print()
    else:
        print("OK  No name changes detected.\n")

    # ── 2. New RUTs on CMF not in DB ──────────────────────────────────────────
    new_ruts = [(rut, name, t) for rut, (name, t) in cmf_all.items() if rut not in db_companies]
    if new_ruts:
        print("i  NEW companies on CMF not yet in DB (download their XBRL files):")
        for rut, name, t in sorted(new_ruts):
            print(f"   {rut}  [{t}]  {name}")
        print()
    else:
        print("OK  No new companies to add.\n")

    # ── 3. DB RUTs missing from CMF ───────────────────────────────────────────
    # Only flag RUTs that have actual financial data (i.e. were real filers, not shells)
    conn = sqlite3.connect(str(DB))
    ruts_with_data = {
        r[0] for r in conn.execute(
            "SELECT DISTINCT c.rut FROM companies c JOIN financials f ON f.company_id = c.id"
        ).fetchall()
    }
    conn.close()

    missing_from_cmf = [
        (rut, db_companies[rut]) for rut in ruts_with_data
        if rut not in cmf_all
    ]
    if missing_from_cmf:
        print("!  RUTs in DB with financial data but NO LONGER on CMF (absorbed / ceased):")
        for rut, name in sorted(missing_from_cmf):
            print(f"   {rut}  {name}")
        print()
    else:
        print("OK  All DB filers are still active on CMF.\n")

    # ── Summary table ─────────────────────────────────────────────────────────
    print("-- Full current CMF registry -----------------------------------------")
    print(f"{'RUT':<16} {'Type':<8} {'Name'}")
    print("-" * 80)
    for rut in sorted(cmf_all):
        name, t = cmf_all[rut]
        marker = " <- NAME CHANGED" if any(r == rut for r, _, _ in name_changes) else ""
        marker = marker or (" <- NEW" if rut not in db_companies else "")
        print(f"{rut:<16} {t:<8} {name}{marker}")


if __name__ == "__main__":
    reconcile()
