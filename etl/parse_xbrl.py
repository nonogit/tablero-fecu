"""
CMF XBRL Parser for Chilean Insurance Companies.

Capture-everything mode: extracts every cl-cs fact whose context ends at
the filing's period_end and loads it into SQLite, alongside a normalized
ramo-level table for BI convenience.

Usage:
    python parse_xbrl.py --input-dir ../202512/descargas --db ../market.db
"""

import argparse
import csv
import os
import re
import sqlite3
import zipfile
from pathlib import Path

CMF_NS_URI = "http://www.cmfchile.cl/cl/fr/cs/2017-11-30"

# Ramo concepts extracted to the normalized ramo table (in addition to financials_all)
RAMO_CONCEPTS = ["PrimasDirectas", "PrimasCedidas", "PrimasRetenidas"]

# Life subtotal handling (unchanged from curated version)
LIFE_SUBTOTALS: set[str] = set()
LIFE_CONDITIONAL_PARENTS = {
    "C421": {"C4211", "C4212"},
    "C422": {"C4221", "C4222"},
}


# ── Context parsing ──────────────────────────────────────────────────────────

def parse_period_from_filename(filename: str) -> tuple[int, int]:
    match = re.search(r"_(\d{6})_", filename)
    if not match:
        raise ValueError(f"Cannot parse period from filename: {filename}")
    yyyymm = match.group(1)
    year = int(yyyymm[:4])
    month = int(yyyymm[4:])
    quarter = (month - 1) // 3 + 1
    return year, quarter


def detect_clcs_prefix(xbrl_text: str) -> str:
    m = re.search(
        rf'xmlns:([A-Za-z][\w-]*)="{re.escape(CMF_NS_URI)}"',
        xbrl_text,
    )
    return m.group(1) if m else "cl-cs"


def parse_contexts_raw(xbrl_text: str) -> dict:
    """
    Regex-based context parser — handles dimensions in <xbrli:scenario> blocks
    which ElementTree traversal misses when namespace prefixes vary.
    Returns context_id → {start, end, instant, dims: {axis: member}}.
    """
    contexts = {}
    for block in re.finditer(
        r'<xbrli:context\s+id="([^"]+)">(.*?)</xbrli:context>',
        xbrl_text, re.DOTALL,
    ):
        ctx_id = block.group(1)
        body = block.group(2)

        start_m = re.search(r'<xbrli:startDate>([^<]+)<', body)
        end_m   = re.search(r'<xbrli:endDate>([^<]+)<', body)
        inst_m  = re.search(r'<xbrli:instant>([^<]+)<', body)

        dims = {}
        for dm in re.finditer(r'dimension="([^"]+)"[^>]*>([^<]+)<', body):
            axis_full = dm.group(1).strip()
            axis      = axis_full.split(":")[-1]
            member    = dm.group(2).strip()
            dims[axis] = member

        contexts[ctx_id] = {
            "start":   start_m.group(1).strip() if start_m else None,
            "end":     end_m.group(1).strip()   if end_m   else None,
            "instant": inst_m.group(1).strip()  if inst_m  else None,
            "dims":    dims,
        }
    return contexts


def identify_primary_contexts(contexts: dict, period_end: str) -> tuple[str | None, str | None]:
    primary_duration = None
    primary_instant  = None

    for ctx_id, ctx in contexts.items():
        if ctx["dims"]:
            continue
        if ctx["instant"] == period_end:
            primary_instant = ctx_id
        if ctx["end"] == period_end and ctx["start"] is not None:
            if primary_duration is None:
                primary_duration = ctx_id
            else:
                existing = contexts[primary_duration]["start"]
                if ctx["start"] < existing:
                    primary_duration = ctx_id

    return primary_duration, primary_instant


def extract_entity_rut(xbrl_text: str) -> str:
    m = re.search(r'<xbrli:identifier[^>]*>([^<]+)<', xbrl_text)
    return m.group(1).strip() if m else "UNKNOWN"


_PREFIX_MAP_CACHE: dict[str, str] | None = None


def _load_rut_prefix_map() -> dict[str, str]:
    """Map 8-digit RUT prefix → canonical RUT (with check digit) from companies.csv."""
    global _PREFIX_MAP_CACHE
    if _PREFIX_MAP_CACHE is not None:
        return _PREFIX_MAP_CACHE
    path = Path(__file__).parent / "companies.csv"
    mapping: dict[str, str] = {}
    if path.exists():
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                r = row["rut"].strip()
                prefix = r.split("-")[0] if "-" in r else r
                mapping[prefix] = r
    _PREFIX_MAP_CACHE = mapping
    return mapping


def canonicalize_rut(filename: str, xbrl_rut: str) -> str:
    """
    Prefer filename prefix (analyst-controlled, matches CMF portal naming)
    over the XBRL payload identifier, since some filings strip the check digit
    and occasionally carry a different filer's RUT in the payload.
    """
    prefix_map = _load_rut_prefix_map()
    fn_m = re.match(r"^(\d{8})", filename)
    if fn_m and fn_m.group(1) in prefix_map:
        return prefix_map[fn_m.group(1)]
    if re.match(r"^\d{8}-[\dKk]$", xbrl_rut):
        return xbrl_rut.upper() if xbrl_rut.endswith("k") else xbrl_rut
    xb_m = re.match(r"^(\d{8})", xbrl_rut)
    if xb_m and xb_m.group(1) in prefix_map:
        return prefix_map[xb_m.group(1)]
    return xbrl_rut


# ── Fact extraction (capture-everything) ─────────────────────────────────────

def _dim_signature(dims: dict) -> str:
    if not dims:
        return ""
    return "|".join(f"{k}={v}" for k, v in sorted(dims.items()))


def extract_all_facts(
    xbrl_text: str,
    contexts: dict,
    period_end: str,
    clcs_prefix: str,
    primary_duration: str | None,
    primary_instant: str | None,
) -> list[dict]:
    """
    Return one dict per fact whose context ends at period_end (duration or instant).
    HTML-block disclosures are skipped naturally — the [^<]*? content group fails
    to match when the text contains nested tags.
    """
    pref = re.escape(clcs_prefix)
    pattern = re.compile(
        rf'<{pref}:([A-Za-z_][\w-]*)\b([^>]*?)(?:/>|>([^<]*?)</{pref}:\1>)',
        re.DOTALL,
    )

    facts: list[dict] = []
    for m in pattern.finditer(xbrl_text):
        concept = m.group(1)
        attrs   = m.group(2)
        raw_val = (m.group(3) or "").strip()

        cref_m = re.search(r'contextRef="([^"]+)"', attrs)
        if cref_m is None:
            continue
        ctx_ref = cref_m.group(1)
        ctx = contexts.get(ctx_ref)
        if ctx is None:
            continue

        if ctx["instant"] is not None:
            if ctx["instant"] != period_end:
                continue
            period_type   = "I"
            context_start = None
            context_end   = ctx["instant"]
        elif ctx["end"] == period_end and ctx["start"] is not None:
            period_type   = "D"
            context_start = ctx["start"]
            context_end   = ctx["end"]
        else:
            continue

        is_primary = (
            (period_type == "D" and ctx_ref == primary_duration and not ctx["dims"])
            or (period_type == "I" and ctx_ref == primary_instant and not ctx["dims"])
        )

        value_num = None
        value_text = None
        if raw_val:
            try:
                value_num = float(raw_val)
            except ValueError:
                value_text = raw_val

        facts.append({
            "concept":       concept,
            "period_type":   period_type,
            "context_start": context_start,
            "context_end":   context_end,
            "dim_signature": _dim_signature(ctx["dims"]),
            "is_primary":    1 if is_primary else 0,
            "value_num":     value_num,
            "value_text":    value_text,
        })

    return facts


# ── Ramo-specific extraction (kept for normalized ramo_code layer) ───────────

def load_fujitsu_item_map(extracted_dir: str) -> dict[str, int]:
    label_files = list(Path(extracted_dir).glob("*-label.xml"))
    if not label_files:
        return {}
    content = label_files[0].read_text(encoding="utf-8-sig", errors="replace")
    mapping = {}
    for item, num in re.findall(
        r'xlink:label="label_(Item\d+)"[^>]*>(\d+)<', content
    ):
        mapping[item] = int(num)
    return mapping


def _ramo_code_from_member(member: str, item_map: dict[str, int]) -> str | None:
    member = member.strip()
    local = member.split(":")[-1]

    if local.startswith("Item"):
        ramo_num = item_map.get(local)
        if ramo_num is None:
            return None
        return f"C{ramo_num}" if ramo_num >= 100 else str(ramo_num)

    m = re.match(r"C601-MargContGene_\w+_(\d+)_\d+", local)
    if m:
        return m.group(1)

    if re.match(r"C\d{3,4}$", local):
        return local

    return None


def extract_ramo_data(
    xbrl_text: str,
    contexts: dict,
    period_end: str,
    item_map: dict[str, int],
    clcs_prefix: str,
) -> dict[tuple[str, str], float]:
    pref = re.escape(clcs_prefix)
    pattern = re.compile(
        rf'<{pref}:(' + '|'.join(re.escape(k) for k in RAMO_CONCEPTS) + r')'
        r'[^>]*contextRef="([^"]+)"[^>]*/?>([^<]*)'
    )

    raw: dict[tuple[str, str], float] = {}
    for m in pattern.finditer(xbrl_text):
        concept, ctx_ref, val_str = m.group(1), m.group(2), m.group(3).strip()
        ctx = contexts.get(ctx_ref)
        if ctx is None or ctx["end"] != period_end:
            continue
        member = ctx["dims"].get("DetalleSubRamosEje")
        if member is None:
            continue
        ramo_code = _ramo_code_from_member(member, item_map)
        if ramo_code is None:
            continue
        try:
            val = float(val_str)
        except ValueError:
            continue
        key = (ramo_code, concept)
        raw[key] = raw.get(key, 0.0) + val

    # Life subtotal dedup
    life_codes = {k[0] for k in raw if not k[0].isdigit()}
    if life_codes:
        for subtotal in LIFE_SUBTOTALS:
            for concept in RAMO_CONCEPTS:
                raw.pop((subtotal, concept), None)
        for parent, children in LIFE_CONDITIONAL_PARENTS.items():
            if children.issubset(life_codes):
                for concept in RAMO_CONCEPTS:
                    raw.pop((parent, concept), None)

    return raw


def detect_insurance_type(xbrl_text: str, contexts: dict, item_map: dict[str, int]) -> str:
    if any(v >= 100 for v in item_map.values()):
        return "Life"
    for ctx in contexts.values():
        member = ctx["dims"].get("DetalleSubRamosEje", "")
        local = member.split(":")[-1]
        if re.match(r"C\d{3,4}$", local):
            return "Life"
        if local.startswith("Item") or "MargContGene" in local:
            return "GI"
    return "Unknown"


# ── File-level pipeline ──────────────────────────────────────────────────────

def parse_xbrl_file(xbrl_path: str) -> dict:
    filename = Path(xbrl_path).name
    year, quarter = parse_period_from_filename(filename)

    quarter_end_month = {1: "03", 2: "06", 3: "09", 4: "12"}
    quarter_end_day   = {1: "31", 2: "30", 3: "30", 4: "31"}
    period_end = f"{year}-{quarter_end_month[quarter]}-{quarter_end_day[quarter]}"

    xbrl_text = Path(xbrl_path).read_bytes().decode("latin-1")
    rut       = canonicalize_rut(filename, extract_entity_rut(xbrl_text))
    contexts  = parse_contexts_raw(xbrl_text)
    clcs_pref = detect_clcs_prefix(xbrl_text)

    dur_ctx, inst_ctx = identify_primary_contexts(contexts, period_end)
    if dur_ctx is None:
        print(f"  WARNING: no primary duration context in {filename}")
    if inst_ctx is None:
        print(f"  WARNING: no primary instant context in {filename}")

    facts = extract_all_facts(
        xbrl_text, contexts, period_end, clcs_pref, dur_ctx, inst_ctx,
    )

    extracted_dir  = str(Path(xbrl_path).parent)
    item_map       = load_fujitsu_item_map(extracted_dir)
    ramo_data      = extract_ramo_data(xbrl_text, contexts, period_end, item_map, clcs_pref)
    insurance_type = detect_insurance_type(xbrl_text, contexts, item_map)

    return {
        "rut":            rut,
        "year":           year,
        "quarter":        quarter,
        "period_end":     period_end,
        "facts":          facts,
        "ramo_data":      ramo_data,
        "insurance_type": insurance_type,
        "source_file":    filename,
    }


def extract_xbrl_from_zip(zip_path: str, extract_dir: str) -> str | None:
    with zipfile.ZipFile(zip_path, "r") as zf:
        xbrl_files = [n for n in zf.namelist() if n.endswith(".xbrl")]
        if not xbrl_files:
            print(f"  WARNING: no .xbrl file in {zip_path}")
            return None
        zf.extractall(extract_dir)
        return os.path.join(extract_dir, xbrl_files[0])


# ── DB layer ─────────────────────────────────────────────────────────────────

def init_db(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS companies (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            rut          TEXT    UNIQUE NOT NULL,
            name         TEXT,
            company_type TEXT
        );

        CREATE TABLE IF NOT EXISTS company_groups (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS company_group_members (
            group_id       INTEGER NOT NULL REFERENCES company_groups(id),
            company_id     INTEGER NOT NULL REFERENCES companies(id),
            effective_from TEXT    NOT NULL,
            effective_to   TEXT,
            PRIMARY KEY (group_id, company_id, effective_from)
        );

        CREATE TABLE IF NOT EXISTS company_successors (
            predecessor_rut TEXT NOT NULL,
            successor_rut   TEXT NOT NULL,
            effective_date  TEXT NOT NULL,
            relationship    TEXT NOT NULL,
            notes           TEXT,
            PRIMARY KEY (predecessor_rut, successor_rut, effective_date)
        );

        CREATE TABLE IF NOT EXISTS periods (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            year     INTEGER NOT NULL,
            quarter  INTEGER NOT NULL,
            end_date TEXT    NOT NULL,
            UNIQUE(year, quarter)
        );

        CREATE TABLE IF NOT EXISTS financials_all (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id     INTEGER NOT NULL REFERENCES companies(id),
            period_id      INTEGER NOT NULL REFERENCES periods(id),
            concept        TEXT    NOT NULL,
            period_type    TEXT    NOT NULL,     -- 'D' / 'I'
            context_start  TEXT,
            context_end    TEXT    NOT NULL,
            dim_signature  TEXT    NOT NULL,
            is_primary     INTEGER NOT NULL DEFAULT 0,
            value_num      REAL,
            value_text     TEXT,
            UNIQUE(company_id, period_id, concept, period_type, context_start, dim_signature)
        );
        CREATE INDEX IF NOT EXISTS idx_fa_primary ON financials_all (company_id, period_id, is_primary);
        CREATE INDEX IF NOT EXISTS idx_fa_concept ON financials_all (concept, period_id);
        CREATE INDEX IF NOT EXISTS idx_fa_dims    ON financials_all (period_id, dim_signature);

        CREATE TABLE IF NOT EXISTS financials_ramo (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id     INTEGER NOT NULL REFERENCES companies(id),
            period_id      INTEGER NOT NULL REFERENCES periods(id),
            insurance_type TEXT    NOT NULL,
            ramo_code      TEXT    NOT NULL,
            concept        TEXT    NOT NULL,
            value          REAL,
            UNIQUE(company_id, period_id, ramo_code, concept)
        );

        CREATE TABLE IF NOT EXISTS dim_ramo (
            insurance_type TEXT NOT NULL,
            ramo_code      TEXT NOT NULL,
            ramo_name      TEXT NOT NULL,
            product_group  TEXT NOT NULL,
            PRIMARY KEY (insurance_type, ramo_code)
        );
    """)
    # Drop legacy financials table if it still exists (subsumed by financials_all)
    conn.execute("DROP TABLE IF EXISTS financials")
    conn.commit()
    conn.close()


def load_dim_ramo(db_path: str):
    csv_path = Path(__file__).parent / "dim_ramo.csv"
    if not csv_path.exists():
        print("  WARNING: dim_ramo.csv not found, skipping dim_ramo load")
        return
    conn = sqlite3.connect(db_path)
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            conn.execute(
                """
                INSERT INTO dim_ramo (insurance_type, ramo_code, ramo_name, product_group)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(insurance_type, ramo_code) DO UPDATE SET
                    ramo_name     = excluded.ramo_name,
                    product_group = excluded.product_group
                """,
                (row["insurance_type"], row["ramo_code"], row["ramo_name"], row["product_group"]),
            )
    conn.commit()
    conn.close()
    print(f"  dim_ramo loaded from {csv_path.name}")


def upsert_record(conn: sqlite3.Connection, parsed: dict):
    cur = conn.cursor()

    cur.execute("INSERT OR IGNORE INTO companies (rut) VALUES (?)", (parsed["rut"],))
    cur.execute("SELECT id FROM companies WHERE rut = ?", (parsed["rut"],))
    company_id = cur.fetchone()[0]

    cur.execute(
        "INSERT OR IGNORE INTO periods (year, quarter, end_date) VALUES (?, ?, ?)",
        (parsed["year"], parsed["quarter"], parsed["period_end"]),
    )
    cur.execute(
        "SELECT id FROM periods WHERE year = ? AND quarter = ?",
        (parsed["year"], parsed["quarter"]),
    )
    period_id = cur.fetchone()[0]

    # financials_all — batched executemany
    rows = [
        (
            company_id, period_id,
            f["concept"], f["period_type"],
            f["context_start"], f["context_end"],
            f["dim_signature"], f["is_primary"],
            f["value_num"], f["value_text"],
        )
        for f in parsed["facts"]
    ]
    cur.executemany(
        """
        INSERT INTO financials_all
            (company_id, period_id, concept, period_type,
             context_start, context_end, dim_signature, is_primary,
             value_num, value_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(company_id, period_id, concept, period_type, context_start, dim_signature)
        DO UPDATE SET
            is_primary = excluded.is_primary,
            value_num  = excluded.value_num,
            value_text = excluded.value_text
        """,
        rows,
    )

    # financials_ramo (normalized)
    for (ramo_code, concept), value in parsed["ramo_data"].items():
        cur.execute(
            """
            INSERT INTO financials_ramo
                (company_id, period_id, insurance_type, ramo_code, concept, value)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_id, period_id, ramo_code, concept) DO UPDATE SET
                value          = excluded.value,
                insurance_type = excluded.insurance_type
            """,
            (company_id, period_id, parsed["insurance_type"], ramo_code, concept, value),
        )

    conn.commit()


def run_etl(input_dir: str, db_path: str):
    init_db(db_path)
    load_dim_ramo(db_path)
    conn = sqlite3.connect(db_path)

    input_path = Path(input_dir)
    files = list(input_path.glob("*.zip")) + list(input_path.glob("*.xbrl"))

    if not files:
        print(f"No .zip or .xbrl files found in {input_dir}")
        return

    for f in sorted(files):
        print(f"Processing: {f.name}")
        if f.suffix == ".zip":
            extract_dir = str(f.parent / (f.stem + "_extracted"))
            os.makedirs(extract_dir, exist_ok=True)
            xbrl_path = extract_xbrl_from_zip(str(f), extract_dir)
            if xbrl_path is None:
                continue
        else:
            xbrl_path = str(f)

        try:
            parsed = parse_xbrl_file(xbrl_path)
            upsert_record(conn, parsed)
            print(
                f"  RUT={parsed['rut']}  {parsed['year']}Q{parsed['quarter']}"
                f"  {parsed['insurance_type']}"
                f"  Facts={len(parsed['facts'])}  Ramos={len(set(k[0] for k in parsed['ramo_data']))}"
            )
        except Exception as e:
            print(f"  ERROR processing {f.name}: {e}")
            raise

    conn.close()
    print(f"\nDone. Database: {db_path}")


def main():
    parser = argparse.ArgumentParser(description="Parse CMF XBRL files into SQLite")
    parser.add_argument("--input-dir", required=True, help="Folder with .zip or .xbrl files")
    parser.add_argument("--db", default="market.db", help="SQLite database path")
    args = parser.parse_args()
    run_etl(args.input_dir, args.db)


if __name__ == "__main__":
    main()
