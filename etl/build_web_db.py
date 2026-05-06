"""
Build a slim market_web.db for the public/web deployment.

Materializes each aggregate view as a real table (so app.py queries work
unchanged) and copies the small dimension tables. Excludes the multi-GB
financials_all / financials_ramo fact tables that the views aggregate.

Usage:  python build_web_db.py
"""

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
SRC  = ROOT / "market.db"
DST  = ROOT / "market_web.db"

DIM_TABLES = [
    "companies",
    "periods",
    "company_groups",
    "company_group_members",
    "company_successors",
    "dim_ramo",
]

VIEWS_TO_MATERIALIZE = [
    "vw_kpis",
    "vw_gwp_by_ramo",
    "vw_gwp_by_product",
    "vw_group_kpis",
    "vw_group_market_share",
    "vw_group_yoy",
    "vw_market_share",
    "vw_market_gwp_by_ramo",
    "vw_market_gwp_by_product",
    "vw_period_yoy",
    "vw_period_qoq",
    "vw_period_comparison",
]


def main():
    if not SRC.exists():
        print(f"ERROR: source DB not found: {SRC}")
        sys.exit(1)

    if DST.exists():
        DST.unlink()

    print(f"Source : {SRC}  ({SRC.stat().st_size / 1e9:.2f} GB)")
    print(f"Dest   : {DST}")

    src = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True)
    dst = sqlite3.connect(DST)

    # 1) Copy dimension tables verbatim (schema + rows)
    for t in DIM_TABLES:
        schema = src.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (t,)
        ).fetchone()
        if not schema or not schema[0]:
            print(f"  skip table  {t} (not in source)")
            continue
        dst.execute(schema[0])
        rows = src.execute(f"SELECT * FROM {t}").fetchall()
        if rows:
            placeholders = ",".join("?" * len(rows[0]))
            dst.executemany(f"INSERT INTO {t} VALUES ({placeholders})", rows)
        print(f"  table   {t:35s} rows={len(rows):>8,}")

    # 2) Materialize each view as a real table of the same name
    for v in VIEWS_TO_MATERIALIZE:
        exists = src.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name=?", (v,)
        ).fetchone()
        if not exists:
            print(f"  skip view   {v} (not in source)")
            continue
        cur = src.execute(f"SELECT * FROM {v}")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        col_defs = ",".join(f'"{c}"' for c in cols)
        dst.execute(f'CREATE TABLE "{v}" ({col_defs})')
        if rows:
            placeholders = ",".join("?" * len(cols))
            dst.executemany(f'INSERT INTO "{v}" VALUES ({placeholders})', rows)
        print(f"  view->tbl {v:35s} rows={len(rows):>8,}")

    dst.commit()
    dst.execute("VACUUM")
    dst.close()
    src.close()

    size_mb = DST.stat().st_size / 1e6
    print(f"\nDone.  market_web.db = {size_mb:.2f} MB")


if __name__ == "__main__":
    main()
