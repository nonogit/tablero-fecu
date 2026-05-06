"""Update company names from companies.csv into market.db."""
import csv
import sqlite3
from pathlib import Path

DB = Path(__file__).parent.parent / "market.db"
CSV = Path(__file__).parent / "companies.csv"

conn = sqlite3.connect(str(DB))
with open(CSV, newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        conn.execute(
            "INSERT INTO companies (rut, name, company_type) VALUES (?, ?, ?) "
            "ON CONFLICT(rut) DO UPDATE SET name = excluded.name, company_type = excluded.company_type",
            (row["rut"], row["name"], row.get("company_type")),
        )
conn.commit()
conn.close()
print("Company names updated.")
