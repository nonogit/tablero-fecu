"""
Quarterly ETL runner.
Usage: python run_quarter.py --period 202512
Expects zip files in ../PERIOD/descargas/
Loads into ../market.db and recreates views.
"""

import argparse
import os
import sqlite3
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Run quarterly CMF ETL")
    parser.add_argument("--period", required=True, help="Period in YYYYMM format, e.g. 202512")
    parser.add_argument("--db", default=None, help="SQLite DB path (default: ../market.db)")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    root_dir = script_dir.parent

    input_dir = root_dir / args.period / "descargas"
    db_path = Path(args.db) if args.db else root_dir / "market.db"
    views_sql = script_dir / "create_views.sql"

    if not input_dir.exists():
        print(f"ERROR: input directory not found: {input_dir}")
        sys.exit(1)

    print(f"=== CMF ETL for period {args.period} ===")
    print(f"Input : {input_dir}")
    print(f"DB    : {db_path}")

    # Step 1: parse XBRL files
    result = subprocess.run(
        [sys.executable, str(script_dir / "parse_xbrl.py"),
         "--input-dir", str(input_dir),
         "--db", str(db_path)],
        check=True
    )

    # Step 2: recreate views
    print("\nRecreating SQL views...")
    conn = sqlite3.connect(str(db_path))
    conn.executescript(views_sql.read_text(encoding="utf-8"))
    conn.close()
    print("Views created: vw_kpis, vw_market_share, vw_period_comparison")

    # Step 3: quick sanity check
    conn = sqlite3.connect(str(db_path))
    rows = conn.execute(
        "SELECT company_name, year, quarter, gwp, loss_ratio_pct, combined_ratio_pct "
        "FROM vw_kpis ORDER BY gwp DESC"
    ).fetchall()
    conn.close()

    print("\n=== Sanity check — vw_kpis ===")
    print(f"{'Company':<20} {'Year':>4} {'Q':>2}  {'GWP (M CLP)':>14}  {'Loss%':>6}  {'Combined%':>9}")
    print("-" * 65)
    for row in rows:
        name, year, q, gwp, lr, cr = row
        gwp_m = f"{gwp/1e9:,.1f}B" if gwp else "n/a"
        lr_s = f"{lr:.1f}%" if lr else "n/a"
        cr_s = f"{cr:.1f}%" if cr else "n/a"
        print(f"{(name or 'unnamed'):<20} {year:>4} {q:>2}  {gwp_m:>14}  {lr_s:>6}  {cr_s:>9}")

    print("\nDone.")


if __name__ == "__main__":
    main()
