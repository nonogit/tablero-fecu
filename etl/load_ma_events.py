"""
Load M&A events and company groups into market.db.
Edit the GROUPS and SUCCESSORS lists below each time a new merger/acquisition occurs.
Then run: python load_ma_events.py

The loader is idempotent: groups present in the DB but no longer in GROUPS are
removed (together with their member links) before re-inserting the desired state.
"""
import sqlite3
from pathlib import Path

DB = Path(__file__).parent.parent / "market.db"

# ── Company groups ────────────────────────────────────────────────────────────
# Each entry: (group_name, [(rut, effective_from, effective_to)])
#   effective_to = None means still active in the group
#   effective_from / effective_to are date strings (YYYY-MM-DD)
# vw_group_kpis filters members by p.end_date >= effective_from AND
#   (effective_to IS NULL OR p.end_date <= effective_to).

GROUPS = [
    (
        "HDI Group",
        [
            # Old HDI entity (99231000-6): filed through Sep 2024, then absorbed
            ("99231000-6", "2000-01-01", "2024-09-30"),
            # Liberty RUT (99061000-2): HDI purchased Liberty, merged everything here,
            # and renamed this entity to HDI Seguros S.A. — ongoing from inception
            ("99061000-2", "2000-01-01", None),
            # Liberty Mutual Surety: new greenfield surety company (unrelated to the merger)
            ("78027718-1", "2025-01-01", None),
        ],
    ),
    (
        "AuguStar Group",
        [
            # AuguStar Seguros de Vida (76632384-7): ex-Zurich Chile Seguros de Rentas
            # Vitalicias, acquired by Constellation Insurance (Ohio National parent)
            # on 2024-12-02 and renamed to AuguStar on that date. Pre-acquisition
            # periods belong to Zurich, not to this group.
            ("76632384-7", "2024-12-02", None),
            # Old Ohio National Chile (96687900-9): merged INTO AuguStar on 2025-08-01.
            # Last filing period = 2025Q2 (end_date 2025-06-30).
            ("96687900-9", "2000-01-01", "2025-06-30"),
        ],
    ),
    (
        "Bice / Security Group",
        [
            # Bice Vida Compañía de Seguros (96656410-5): surviving entity of the
            # Bice Vida + Vida Security merger (CMF authorized 2025-12-18, effective
            # 2026-01-01). Group starts when the merger agreement was approved by
            # shareholders — for cleanliness, set from 2000-01-01 since Bice has
            # always been Bice. Vida Security joins the group retroactively so
            # historical YoY sums both.
            ("96656410-5", "2000-01-01", None),
            # Vida Security (99301000-6): absorbed by Bice effective 2026-01-01.
            # Last filing period = 2025Q4 (end_date 2025-12-31).
            ("99301000-6", "2000-01-01", "2025-12-31"),
        ],
    ),
]

# ── Successor chain ───────────────────────────────────────────────────────────
# (predecessor_rut, successor_rut, effective_date, relationship, notes)

SUCCESSORS = [
    (
        "99231000-6",
        "99061000-2",
        "2024-10-01",
        "merger",
        "HDI purchased Liberty Seguros Generales. The combined business was consolidated "
        "into the Liberty RUT (99061000-2) and that entity was renamed HDI Seguros S.A. "
        "The old HDI entity (99231000-6) ceased filing after Sep 2024 (9m GWP 380B CLP). "
        "Liberty RUT Dec 2024 YTD GWP (422B) reflects ~3 months of absorbed HDI premiums.",
    ),
    (
        "78027718-1",
        "78027718-1",
        "2025-01-01",
        "new_entity",
        "New surety-only company. CMF resolution 11839 dated 17-Dec-2024, constituted 03-Jan-2025.",
    ),
    (
        "96687900-9",
        "76632384-7",
        "2025-08-01",
        "merger",
        "Ohio National Seguros de Vida (Chile) merged into AuguStar Seguros de Vida "
        "(76632384-7, itself the rebranded ex-Zurich Chile Seguros de Rentas Vitalicias "
        "that Constellation Insurance acquired on 2024-12-02). CMF approved Jul 2025; "
        "the unified entity began operating under the AuguStar name on 2025-08-01. "
        "Ohio National's last filing was 2025Q2 (YTD GWP 91.9B CLP).",
    ),
    (
        "99301000-6",
        "96656410-5",
        "2026-01-01",
        "merger",
        "Seguros Vida Security Previsión was absorbed by Bice Vida as part of the "
        "BICECORP / Grupo Security corporate merger. Shareholders approved the fusion; "
        "CMF authorized on 2025-12-18; legally effective 2026-01-01. Vida Security's "
        "last filing is expected to be 2025Q4 (end_date 2025-12-31).",
    ),
]


def run():
    conn = sqlite3.connect(str(DB))
    cur = conn.cursor()

    desired_group_names = [name for name, _ in GROUPS]

    # Idempotent cleanup: drop memberships and groups no longer in the desired list.
    # (Successors are append-only and keyed — we let ON CONFLICT DO NOTHING handle them.)
    placeholders = ",".join("?" * len(desired_group_names)) or "''"
    cur.execute(
        f"""
        DELETE FROM company_group_members
        WHERE group_id IN (
            SELECT id FROM company_groups
            WHERE group_name NOT IN ({placeholders})
        )
        """,
        desired_group_names,
    )
    cur.execute(
        f"""
        DELETE FROM company_groups
        WHERE group_name NOT IN ({placeholders})
        """,
        desired_group_names,
    )

    # For the groups we do want, clear their existing members so we can rewrite
    # the effective_from/effective_to cleanly in a single pass.
    cur.execute(
        f"""
        DELETE FROM company_group_members
        WHERE group_id IN (
            SELECT id FROM company_groups WHERE group_name IN ({placeholders})
        )
        """,
        desired_group_names,
    )

    # Insert groups and members
    for group_name, members in GROUPS:
        cur.execute(
            "INSERT INTO company_groups (group_name) VALUES (?) "
            "ON CONFLICT(group_name) DO NOTHING",
            (group_name,),
        )
        cur.execute("SELECT id FROM company_groups WHERE group_name = ?", (group_name,))
        group_id = cur.fetchone()[0]

        for rut, eff_from, eff_to in members:
            cur.execute("SELECT id FROM companies WHERE rut = ?", (rut,))
            row = cur.fetchone()
            if row is None:
                print(f"  WARNING: RUT {rut} not found in companies — skipping group membership")
                continue
            company_id = row[0]
            cur.execute(
                """
                INSERT INTO company_group_members (group_id, company_id, effective_from, effective_to)
                VALUES (?, ?, ?, ?)
                """,
                (group_id, company_id, eff_from, eff_to),
            )

    # Successors — append-only, safe to replay
    for row in SUCCESSORS:
        cur.execute(
            """
            INSERT INTO company_successors
                (predecessor_rut, successor_rut, effective_date, relationship, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(predecessor_rut, successor_rut, effective_date) DO UPDATE SET
                relationship = excluded.relationship,
                notes        = excluded.notes
            """,
            row,
        )

    conn.commit()
    conn.close()
    print(f"M&A events loaded: {len(GROUPS)} groups, {len(SUCCESSORS)} successor records.")


if __name__ == "__main__":
    run()
