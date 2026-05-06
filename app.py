"""
CMF Market Dashboard — Streamlit prototype
Template to be reproduced in Power BI.
"""
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# ── Constants ────────────────────────────────────────────────────────────────
_HERE            = Path(__file__).parent
DB_PATH          = _HERE / "market_web.db" if (_HERE / "market_web.db").exists() else _HERE / "market.db"
ZURICH_RUTS      = {"99037000-1", "76590840-K", "96819630-8", "99185000-7"}
ZURICH_CORE_RUTS = {"99037000-1", "99185000-7"}
ZURICH_BLUE      = "#1B3F8F"
GRAY             = "#BBBBBB"
MM               = 1_000_000
FX_CLP_PER_USD   = 961.8159
MONEY_COLS       = {
    "gwp", "ceded_premiums", "net_premiums", "gwp_earned", "net_earned_premium",
    "claims_sinistros", "claims_rentas", "claims_cost", "claims_paid", "claims_recoveries",
    "admin_costs", "intermediation_result", "technical_result", "investment_result",
    "net_result", "nep", "prev_gwp", "prev_nep", "mkt_gwp", "z_gwp",
}
TYPE_LABELS      = {"general": "Generales", "life": "Vida", "surety": "Fianzas"}
TYPE_COLORS      = {"general": "#1B3F8F", "life": "#2E8B57", "surety": "#E07B00"}
ALL_TYPES        = list(TYPE_LABELS.keys())

_SUM_COLS = [
    "gwp", "ceded_premiums", "net_premiums", "gwp_earned", "net_earned_premium",
    "claims_sinistros", "claims_rentas", "claims_cost", "claims_paid", "claims_recoveries",
    "admin_costs", "intermediation_result", "technical_result", "investment_result",
    "net_result", "nep",
]

# ── DB helpers ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    con = sqlite3.connect(DB_PATH)
    df  = pd.read_sql(sql, con, params=params)
    con.close()
    return df


def table_exists(name: str) -> bool:
    res = q("SELECT count(*) AS n FROM sqlite_master WHERE type='table' OR type='view' AND name=?", (name,))
    return int(res["n"].iloc[0]) > 0


@st.cache_data(ttl=300)
def _fetch_kpis(year: int, quarter: int) -> pd.DataFrame:
    return q("SELECT * FROM vw_kpis WHERE year=? AND quarter=?", (year, quarter))


@st.cache_data(ttl=300)
def _fetch_ramo(year: int, quarter: int) -> pd.DataFrame:
    return q("SELECT * FROM vw_gwp_by_ramo WHERE year=? AND quarter=?", (year, quarter))


@st.cache_data(ttl=300)
def _fetch_group_kpis(year: int, quarter: int) -> pd.DataFrame:
    return q("SELECT * FROM vw_group_kpis WHERE year=? AND quarter=?", (year, quarter))


@st.cache_data(ttl=300)
def _fetch_group_ms(year: int, quarter: int) -> pd.DataFrame:
    return q("SELECT * FROM vw_group_market_share WHERE year=? AND quarter=?", (year, quarter))


def fmt_pct(v) -> str:
    return "—" if pd.isna(v) else f"{v:.1f}%"


def period_label(year: int, quarter: int) -> str:
    return f"{year} Q{quarter}"


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="CMF Market Dashboard", layout="wide", initial_sidebar_state="expanded")


# ── Password gate ─────────────────────────────────────────────────────────────
def _check_password() -> bool:
    try:
        expected = st.secrets["password"]
    except Exception:
        return True  # No secret configured -> open (local dev)

    if st.session_state.get("auth_ok"):
        return True

    def _attempt():
        if st.session_state.get("pw_input") == expected:
            st.session_state["auth_ok"] = True
            st.session_state["pw_input"] = ""
        else:
            st.session_state["auth_ok"] = False

    st.title("CMF Market Dashboard")
    st.text_input("Contraseña", type="password", key="pw_input", on_change=_attempt)
    if st.session_state.get("auth_ok") is False:
        st.error("Contraseña incorrecta.")
    return False


if not _check_password():
    st.stop()


st.sidebar.title("CMF Market Dashboard")

# ── Section ───────────────────────────────────────────────────────────────────
section = st.sidebar.radio("Sección", ["Compañías", "Corredores"], horizontal=True)

st.sidebar.markdown("---")

# ── Page (same list for both sections) ───────────────────────────────────────
PAGES = ["Market Overview", "Zurich Focus", "Ramo Deep-dive", "YoY Trends", "M&A Groups"]
page  = st.sidebar.selectbox("Página", PAGES)

# ── Period filter ─────────────────────────────────────────────────────────────
periods_df    = q("SELECT id, year, quarter, end_date FROM periods ORDER BY year DESC, quarter DESC")
period_labels = [period_label(r.year, r.quarter) for r in periods_df.itertuples()]

sel_label = st.sidebar.selectbox("Período", period_labels, index=0)
sel_row   = periods_df.iloc[period_labels.index(sel_label)]
SEL_YEAR, SEL_QTR = int(sel_row.year), int(sel_row.quarter)

# ── Currency toggle ───────────────────────────────────────────────────────────
CURRENCY        = st.sidebar.radio("Moneda", ["CLP", "USD"], horizontal=True, key="currency")
_IS_USD         = CURRENCY == "USD"
CUR_PREFIX_MM   = "MMUSD$" if _IS_USD else "MM$"
CUR_AXIS_LABEL  = f"GWP ({CURRENCY})"
CUR_COL_GWP     = f"GWP {CUR_PREFIX_MM}"
CUR_COL_GWP_PRV = f"GWP prev {CUR_PREFIX_MM}"
CUR_COL_NEP     = f"NEP {CUR_PREFIX_MM}"


def apply_fx(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all monetary columns from CLP to USD if USD is selected."""
    if not _IS_USD or df is None or df.empty:
        return df
    cols = list(set(df.columns) & MONEY_COLS)
    if not cols:
        return df
    df = df.copy()
    df[cols] = df[cols] / FX_CLP_PER_USD
    return df


def fmt_money_mm(v) -> str:
    """Format an already-converted monetary value in millions of selected currency."""
    if pd.isna(v):
        return "—"
    return f"{CUR_PREFIX_MM} {v / MM:,.0f}"


def fmt_money_compact(v) -> str:
    """Compact money for metric tiles: auto-scales to K / M / B / T."""
    if pd.isna(v):
        return "—"
    abs_v = abs(v)
    if abs_v >= 1e12:
        return f"{v / 1e12:,.2f}T"
    if abs_v >= 1e9:
        return f"{v / 1e9:,.2f}B"
    if abs_v >= 1e6:
        return f"{v / 1e6:,.1f}M"
    if abs_v >= 1e3:
        return f"{v / 1e3:,.1f}K"
    return f"{v:,.0f}"


# ── Segment filter ────────────────────────────────────────────────────────────
sel_types = st.sidebar.multiselect(
    "Segmento", ALL_TYPES, default=ALL_TYPES, format_func=TYPE_LABELS.get,
)
if not sel_types:
    st.sidebar.warning("Selecciona al menos un segmento.")
    st.stop()

# ── Ramo filter ───────────────────────────────────────────────────────────────
_type_to_ins  = {"general": "GI", "life": "Life", "surety": "GI"}
_sel_ins      = {_type_to_ins[t] for t in sel_types}
_pg_df        = q("SELECT DISTINCT insurance_type, product_group FROM vw_gwp_by_ramo ORDER BY product_group")
_available_pg = sorted(_pg_df[_pg_df["insurance_type"].isin(_sel_ins)]["product_group"].unique().tolist())

sel_product_groups = st.sidebar.multiselect(
    "Ramo", _available_pg, default=_available_pg,
    key=f"ramo_{'_'.join(sorted(sel_types))}",
)
if not sel_product_groups:
    st.sidebar.warning("Selecciona al menos un ramo.")
    st.stop()

# ── Company filter ────────────────────────────────────────────────────────────
_companies_ref  = q("SELECT rut, company_type FROM companies")                         # for type-merge in pages
_companies_full = q("SELECT rut, name AS company_name, company_type FROM companies ORDER BY name")  # for filter widget
_seg_companies  = _companies_full[_companies_full["company_type"].isin(sel_types)]
_available_cos  = _seg_companies["company_name"].tolist()

sel_companies = st.sidebar.multiselect(
    "Compañía (vacío = todas)", _available_cos, default=[],
    key=f"company_{'_'.join(sorted(sel_types))}",
)

st.sidebar.caption(
    f"Valores en {CUR_PREFIX_MM} (millones {CURRENCY}) · Cifras acumuladas YTD"
    f" · FX: {FX_CLP_PER_USD:,.4f} CLP/USD"
)

# ── Filter state ──────────────────────────────────────────────────────────────
_seg_ruts: set | None = (
    None if set(sel_types) == set(ALL_TYPES)
    else set(_companies_ref.loc[_companies_ref["company_type"].isin(sel_types), "rut"])
)
_company_ruts: set | None = (
    None if not sel_companies
    else set(_companies_full.loc[_companies_full["company_name"].isin(sel_companies), "rut"])
)
_ramo_active = set(sel_product_groups) != set(_available_pg)


def ftype(df: pd.DataFrame) -> pd.DataFrame:
    if _seg_ruts is None or "rut" not in df.columns:
        return df
    return df[df["rut"].isin(_seg_ruts)]


def framo(df: pd.DataFrame) -> pd.DataFrame:
    if not _ramo_active or "product_group" not in df.columns:
        return df
    return df[df["product_group"].isin(sel_product_groups)]


def fcompany(df: pd.DataFrame) -> pd.DataFrame:
    if _company_ruts is None or "rut" not in df.columns:
        return df
    return df[df["rut"].isin(_company_ruts)]


def _recompute_ratios(df: pd.DataFrame) -> pd.DataFrame:
    denom = df["nep"].where(df["nep"].fillna(0) != 0, df.get("net_premiums", pd.Series(dtype=float)))
    df = df.copy()
    df["loss_ratio_pct"]     = df["claims_cost"]   / denom * 100
    df["expense_ratio_pct"]  = df["admin_costs"]    / denom * 100
    df["combined_ratio_pct"] = df["loss_ratio_pct"] + df["expense_ratio_pct"]
    df["net_margin_pct"]     = df["net_result"]     / df["gwp"] * 100
    df["cession_rate_pct"]   = df["ceded_premiums"] / df["gwp"] * 100
    return df


def get_kpis() -> pd.DataFrame:
    combined = fcompany(ftype(_fetch_kpis(SEL_YEAR, SEL_QTR)))
    if _ramo_active:
        ramo_gwp = framo(fcompany(ftype(_fetch_ramo(SEL_YEAR, SEL_QTR)))) \
                       .groupby("rut")["gwp"].sum().reset_index().rename(columns={"gwp": "gwp_ramo"})
        combined = combined.merge(ramo_gwp, on="rut", how="inner")
        combined["gwp"] = combined["gwp_ramo"]
        combined["net_margin_pct"] = combined["net_result"] / combined["gwp"] * 100
        combined = combined.drop(columns=["gwp_ramo"])
    return apply_fx(combined)


_RAMO_KEYS = ["rut", "company_name", "company_type", "insurance_type", "ramo_code", "ramo_name", "product_group"]

def get_ramo(zurich_only: bool = False) -> pd.DataFrame:
    df = fcompany(ftype(_fetch_ramo(SEL_YEAR, SEL_QTR)))
    if zurich_only:
        df = df[df["rut"].isin(ZURICH_RUTS)]
    return apply_fx(framo(df))


_GRP_NUM = ["gwp", "ceded_premiums", "net_premiums", "nep", "claims_cost",
            "admin_costs", "technical_result", "investment_result", "net_result"]

def get_group_kpis() -> pd.DataFrame:
    return apply_fx(_fetch_group_kpis(SEL_YEAR, SEL_QTR))


def period_badge() -> str:
    return sel_label


def seg_badge() -> str:
    labels = [TYPE_LABELS[t] for t in ALL_TYPES if t in sel_types]
    return "  |  " + " · ".join(labels)


def ramo_badge() -> str:
    if not _ramo_active:
        return ""
    n, total = len(sel_product_groups), len(_available_pg)
    return "  |  " + (" · ".join(sel_product_groups) if n <= 3 else f"{n}/{total} ramos")


def company_badge() -> str:
    if not sel_companies:
        return ""
    n = len(sel_companies)
    return "  |  " + (", ".join(shorten(pd.Series(sel_companies)).tolist()) if n <= 2 else f"{n} compañías")


def shorten(s: pd.Series) -> pd.Series:
    return s.str.replace(
        r"Seguros?\s*de\s*|Seguros?\s*|Compañía\s*de\s*|S\.A\.|Chile\s*", "", regex=True
    ).str.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: COMPAÑÍAS
# ═══════════════════════════════════════════════════════════════════════════════
if section == "Compañías":

    # ── Market Overview ───────────────────────────────────────────────────────
    if page == "Market Overview":
        st.title(f"Market Overview — {period_badge()}{seg_badge()}{ramo_badge()}{company_badge()}")

        kpis = get_kpis().merge(_companies_ref, on="rut", how="left")
        if kpis.empty:
            st.warning("Sin datos para este período / filtro.")
            st.stop()

        mkt_gwp = kpis["gwp"].sum()
        n_cos   = len(kpis)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric(f"GWP mercado ({CURRENCY})", fmt_money_compact(mkt_gwp))
        c2.metric("Compañías", n_cos)
        c3.metric("Loss Ratio promedio", fmt_pct(kpis["loss_ratio_pct"].mean()))
        c4.metric("Combined Ratio promedio", fmt_pct(kpis["combined_ratio_pct"].mean()))

        top_n = st.slider("Top N compañías en ranking", 10, n_cos, min(30, n_cos)) if n_cos > 10 else n_cos
        top   = kpis.nlargest(top_n, "gwp").reset_index(drop=True)
        top["rank"]       = top.index + 1
        top["Segmento"]   = top["company_type"].map(TYPE_LABELS)
        top["short_name"] = shorten(top["company_name"])
        top["label"]      = top.apply(
            lambda r: f"{r['rank']}. ★ {r['short_name']}" if r["rut"] in ZURICH_RUTS
                      else f"{r['rank']}. {r['short_name']}", axis=1,
        )

        col_bar, col_donut = st.columns([3, 1])
        with col_bar:
            fig = px.bar(top, x="gwp", y="label", orientation="h", color="Segmento",
                         color_discrete_map={v: TYPE_COLORS[k] for k, v in TYPE_LABELS.items()},
                         title=f"GWP Ranking — Top {top_n}",
                         labels={"gwp": CUR_AXIS_LABEL, "label": ""})
            fig.update_xaxes(tickformat=".3s")
            fig.update_layout(yaxis={"categoryorder": "total ascending"},
                              legend_title_text="Segmento", height=max(400, top_n * 22))
            st.plotly_chart(fig, use_container_width=True)

        with col_donut:
            top10      = kpis.nlargest(10, "gwp").copy()
            others_gwp = mkt_gwp - top10["gwp"].sum()
            if others_gwp > 0:
                top10 = pd.concat(
                    [top10, pd.DataFrame([{"company_name": "Otros", "gwp": others_gwp}])],
                    ignore_index=True,
                )
            fig2 = px.pie(top10, values="gwp", names="company_name", hole=0.45,
                          title="Market share (top 10 + otros)")
            fig2.update_traces(textposition="inside", textinfo="percent+label")
            fig2.update_layout(showlegend=False, height=480)
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("GWP por segmento")
        by_type = kpis.groupby("company_type")["gwp"].sum().reset_index().sort_values("gwp", ascending=False)
        by_type["Segmento"] = by_type["company_type"].map(TYPE_LABELS)
        fig3 = px.bar(by_type, x="Segmento", y="gwp", color="Segmento",
                      color_discrete_map={v: TYPE_COLORS[k] for k, v in TYPE_LABELS.items()},
                      labels={"gwp": CUR_AXIS_LABEL}, title="GWP por segmento")
        fig3.update_yaxes(tickformat=".3s")
        fig3.update_layout(showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

    # ── Zurich Focus ──────────────────────────────────────────────────────────
    elif page == "Zurich Focus":
        st.title(f"Zurich Focus — {period_badge()}{seg_badge()}{ramo_badge()}{company_badge()}")

        all_kpis = get_kpis()
        z_kpis   = all_kpis[all_kpis["rut"].isin(ZURICH_CORE_RUTS)].copy()
        mkt_gwp  = all_kpis["gwp"].sum()

        if z_kpis.empty:
            st.warning("Sin datos Zurich para este período / filtro.")
            st.stop()

        st.subheader("Entidades Zurich")
        for _, row in z_kpis.iterrows():
            with st.expander(row["company_name"], expanded=True):
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric(f"GWP ({CURRENCY})", fmt_money_compact(row['gwp']))
                c2.metric("Loss Ratio", fmt_pct(row["loss_ratio_pct"]))
                c3.metric("Combined Ratio", fmt_pct(row["combined_ratio_pct"]))
                c4.metric("Net Margin", fmt_pct(row["net_margin_pct"]))
                c5.metric("Market Share", fmt_pct(row["gwp"] / mkt_gwp * 100 if mkt_gwp else None))

        st.subheader("Comparación con pares")
        non_z = all_kpis[~all_kpis["rut"].isin(ZURICH_CORE_RUTS)].nlargest(30, "gwp")
        selected_peers = st.multiselect(
            "Seleccionar pares", non_z["company_name"].tolist(),
            default=non_z["company_name"].tolist()[:5],
        )
        metric_options = {
            "gwp": "GWP", "loss_ratio_pct": "Loss Ratio %",
            "combined_ratio_pct": "Combined Ratio %", "net_margin_pct": "Net Margin %",
            "expense_ratio_pct": "Expense Ratio %",
        }
        sel_metric = st.selectbox("Métrica", list(metric_options.keys()), format_func=metric_options.get)
        compare = pd.concat([z_kpis, all_kpis[all_kpis["company_name"].isin(selected_peers)]]) \
                    .sort_values(sel_metric, ascending=False)
        compare["short_name"] = shorten(compare["company_name"])
        fig = px.bar(compare, x="short_name", y=sel_metric,
                     color=compare["rut"].isin(ZURICH_CORE_RUTS),
                     color_discrete_map={True: ZURICH_BLUE, False: GRAY},
                     title=f"{metric_options[sel_metric]} — Zurich vs pares",
                     labels={"short_name": "", sel_metric: metric_options[sel_metric]})
        if sel_metric == "gwp":
            fig.update_yaxes(tickformat=".3s")
        fig.update_layout(showlegend=False, xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("YoY — Zurich")
        if _ramo_active:
            st.info("YoY no disponible con filtro de ramo activo.")
        else:
            yoy = apply_fx(ftype(q(
                "SELECT * FROM vw_period_yoy WHERE year=? AND quarter=? AND rut IN ('99037000-1','99185000-7')",
                (SEL_YEAR, SEL_QTR),
            )))
            if yoy.empty:
                st.info("Sin datos YoY para este período.")
            else:
                disp = yoy[["company_name", "gwp", "prev_gwp", "gwp_yoy_pct",
                             "loss_ratio_pct", "prev_loss_ratio_pct",
                             "combined_ratio_pct", "prev_combined_ratio_pct"]].copy()
                disp[CUR_COL_GWP]     = (disp["gwp"] / MM).round(0)
                disp[CUR_COL_GWP_PRV] = (disp["prev_gwp"] / MM).round(0)
                st.dataframe(
                    disp[["company_name", CUR_COL_GWP, CUR_COL_GWP_PRV, "gwp_yoy_pct",
                           "loss_ratio_pct", "prev_loss_ratio_pct",
                           "combined_ratio_pct", "prev_combined_ratio_pct"]]
                    .rename(columns={
                        "company_name": "Compañía", "gwp_yoy_pct": "GWP YoY %",
                        "loss_ratio_pct": "LR %", "prev_loss_ratio_pct": "LR prev %",
                        "combined_ratio_pct": "CR %", "prev_combined_ratio_pct": "CR prev %",
                    }),
                    use_container_width=True,
                )

    # ── Ramo Deep-dive ────────────────────────────────────────────────────────
    elif page == "Ramo Deep-dive":
        st.title(f"Ramo Deep-dive — {period_badge()}{seg_badge()}{ramo_badge()}{company_badge()}")

        scope = st.radio("Alcance", ["Mercado completo", "Solo Zurich"], horizontal=True)
        ramo  = get_ramo(zurich_only=(scope == "Solo Zurich"))

        if ramo.empty:
            st.warning("Sin datos de ramo para este período / filtro.")
            st.stop()

        by_product = ramo.groupby("product_group")["gwp"].sum().reset_index().sort_values("gwp", ascending=False)
        fig1 = px.bar(by_product, x="product_group", y="gwp",
                      title=f"GWP por grupo de producto ({scope})",
                      labels={"gwp": CUR_AXIS_LABEL, "product_group": ""},
                      color_discrete_sequence=[ZURICH_BLUE])
        fig1.update_yaxes(tickformat=".3s")
        fig1.update_layout(xaxis_tickangle=-25)
        st.plotly_chart(fig1, use_container_width=True)

        by_ramo = ramo.groupby("ramo_name")["gwp"].sum().reset_index().nlargest(25, "gwp")
        fig2 = px.bar(by_ramo, x="gwp", y="ramo_name", orientation="h",
                      title=f"GWP por ramo — top 25 ({scope})",
                      labels={"gwp": CUR_AXIS_LABEL, "ramo_name": ""},
                      color_discrete_sequence=[ZURICH_BLUE])
        fig2.update_xaxes(tickformat=".3s")
        fig2.update_layout(yaxis={"categoryorder": "total ascending"}, height=620)
        st.plotly_chart(fig2, use_container_width=True)

        if scope == "Mercado completo":
            st.subheader("Market share Zurich por ramo (top 25 ramos del mercado)")
            mkt_by_ramo = ramo.groupby("ramo_name")["gwp"].sum().rename("mkt_gwp")
            z_by_ramo   = ramo[ramo["rut"].isin(ZURICH_RUTS)].groupby("ramo_name")["gwp"].sum().rename("z_gwp")
            share_df    = pd.concat([mkt_by_ramo, z_by_ramo], axis=1).fillna(0).reset_index()
            share_df["share_pct"] = share_df["z_gwp"] / share_df["mkt_gwp"] * 100
            share_df = share_df.nlargest(25, "mkt_gwp")
            fig3 = px.bar(share_df, x="share_pct", y="ramo_name", orientation="h",
                          title="Market share Zurich % por ramo",
                          labels={"share_pct": "Share %", "ramo_name": ""},
                          color_discrete_sequence=[ZURICH_BLUE])
            fig3.update_layout(yaxis={"categoryorder": "total ascending"}, height=620)
            st.plotly_chart(fig3, use_container_width=True)

    # ── YoY Trends ────────────────────────────────────────────────────────────
    elif page == "YoY Trends":
        st.title(f"YoY Trends{seg_badge()}{ramo_badge()}{company_badge()}")

        all_period_kpis = pd.concat(
            [apply_fx(ftype(_fetch_kpis(int(r.year), int(r.quarter)))) for r in periods_df.itertuples()],
            ignore_index=True,
        )
        if _ramo_active:
            ramo_all = pd.concat(
                [apply_fx(framo(ftype(_fetch_ramo(int(r.year), int(r.quarter))))).assign(year=int(r.year), quarter=int(r.quarter))
                 for r in periods_df.itertuples()],
                ignore_index=True,
            )
            ramo_gwp = ramo_all.groupby(["rut", "year", "quarter"])["gwp"].sum().reset_index().rename(columns={"gwp": "gwp_ramo"})
            all_period_kpis = all_period_kpis.merge(ramo_gwp, on=["rut", "year", "quarter"], how="inner")
            all_period_kpis["gwp"] = all_period_kpis["gwp_ramo"]
            all_period_kpis = all_period_kpis.drop(columns=["gwp_ramo"])

        all_names    = sorted(all_period_kpis["company_name"].unique().tolist())
        zurich_defs  = [n for n in all_names if "Zurich" in n]
        selected_cos = st.multiselect("Compañías", all_names, default=zurich_defs)

        metric_map = {
            "gwp": "GWP", "net_earned_premium": "NEP",
            "loss_ratio_pct": "Loss Ratio %", "combined_ratio_pct": "Combined Ratio %",
            "net_margin_pct": "Net Margin %", "expense_ratio_pct": "Expense Ratio %",
        }
        sel_metric = st.selectbox("Métrica", list(metric_map.keys()), format_func=metric_map.get)

        if not selected_cos:
            st.info("Selecciona al menos una compañía.")
            st.stop()

        plot_df = all_period_kpis[all_period_kpis["company_name"].isin(selected_cos)].copy()
        plot_df = plot_df.merge(_companies_ref, on="rut", how="left")
        plot_df["período"]  = plot_df["year"].astype(str) + " Q" + plot_df["quarter"].astype(str)
        plot_df["Segmento"] = plot_df["company_type"].map(TYPE_LABELS)
        plot_df = plot_df.sort_values(["company_name", "year", "quarter"])

        fig = px.line(plot_df, x="período", y=sel_metric, color="company_name",
                      line_dash="Segmento", markers=True,
                      title=f"{metric_map[sel_metric]} — evolución temporal",
                      labels={"período": "Período", sel_metric: metric_map[sel_metric], "company_name": "Compañía"})
        if sel_metric in ("gwp", "net_earned_premium"):
            fig.update_yaxes(tickformat=".3s")
        st.plotly_chart(fig, use_container_width=True)

        if _ramo_active:
            st.info("GWP YoY % no disponible con filtro de ramo activo.")
        else:
            yoy_all = apply_fx(ftype(q("SELECT * FROM vw_period_yoy WHERE year=? AND quarter=?", (SEL_YEAR, SEL_QTR))))
            yoy_sel = yoy_all[yoy_all["company_name"].isin(selected_cos)].copy()
            if not yoy_sel.empty:
                yoy_sel["período"] = yoy_sel["year"].astype(str) + " Q" + yoy_sel["quarter"].astype(str)
                fig2 = px.bar(yoy_sel, x="período", y="gwp_yoy_pct", color="company_name",
                              barmode="group", title="GWP YoY % — variación anual",
                              labels={"período": "Período", "gwp_yoy_pct": "YoY %", "company_name": "Compañía"})
                fig2.add_hline(y=0, line_dash="dot", line_color="gray")
                st.plotly_chart(fig2, use_container_width=True)

    # ── M&A Groups ────────────────────────────────────────────────────────────
    elif page == "M&A Groups":
        st.title(f"M&A Groups — {period_badge()}{seg_badge()}")
        if _ramo_active or set(sel_types) != set(ALL_TYPES):
            st.info("Filtros de segmento y ramo no aplican a esta vista.")

        g_kpis = get_group_kpis()
        if g_kpis.empty:
            st.warning("Sin datos de grupos para este período.")
            st.stop()

        st.subheader("KPIs por grupo")
        disp = g_kpis.copy()
        disp[CUR_COL_GWP] = (disp["gwp"] / MM).round(0)
        disp[CUR_COL_NEP] = (disp["nep"] / MM).round(0)
        st.dataframe(
            disp[["group_name", CUR_COL_GWP, CUR_COL_NEP, "loss_ratio_pct", "expense_ratio_pct",
                   "combined_ratio_pct", "net_margin_pct"]]
            .rename(columns={"group_name": "Grupo", "loss_ratio_pct": "LR %",
                             "expense_ratio_pct": "Exp R %", "combined_ratio_pct": "CR %",
                             "net_margin_pct": "NM %"}),
            use_container_width=True,
        )

        g_ms = apply_fx(_fetch_group_ms(SEL_YEAR, SEL_QTR))
        if True:
            if not g_ms.empty:
                fig_donut = px.pie(g_ms, values="gwp", names="group_name", hole=0.45,
                                   title=f"Market share por grupo — {period_badge()}")
                fig_donut.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(fig_donut, use_container_width=True)

        st.subheader("Evolución GWP por grupo")
        g_all = apply_fx(q("SELECT * FROM vw_group_kpis ORDER BY year, quarter"))
        g_all["período"] = g_all["year"].astype(str) + " Q" + g_all["quarter"].astype(str)
        g_all = g_all.sort_values(["group_name", "year", "quarter"])
        fig_t = px.line(g_all, x="período", y="gwp", color="group_name", markers=True,
                        title="GWP por grupo — serie temporal",
                        labels={"período": "Período", "gwp": CUR_AXIS_LABEL, "group_name": "Grupo"})
        fig_t.update_yaxes(tickformat=".3s")
        st.plotly_chart(fig_t, use_container_width=True)

        st.subheader("YoY por grupo")
        g_yoy = apply_fx(q("SELECT * FROM vw_group_yoy WHERE year=? AND quarter=?", (SEL_YEAR, SEL_QTR)))
        if True:
            if g_yoy.empty:
                st.info("Sin datos YoY para este período.")
            else:
                g_yoy[CUR_COL_GWP]     = (g_yoy["gwp"] / MM).round(0)
                g_yoy[CUR_COL_GWP_PRV] = (g_yoy["prev_gwp"] / MM).round(0)
                st.dataframe(
                    g_yoy[["group_name", CUR_COL_GWP, CUR_COL_GWP_PRV, "gwp_yoy_pct",
                            "loss_ratio_pct", "prev_loss_ratio_pct",
                            "combined_ratio_pct", "prev_combined_ratio_pct"]]
                    .rename(columns={
                        "group_name": "Grupo", "gwp_yoy_pct": "GWP YoY %",
                        "loss_ratio_pct": "LR %", "prev_loss_ratio_pct": "LR prev %",
                        "combined_ratio_pct": "CR %", "prev_combined_ratio_pct": "CR prev %",
                    }),
                    use_container_width=True,
                )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION: CORREDORES
# ═══════════════════════════════════════════════════════════════════════════════
else:  # section == "Corredores"

    # Check if broker data has been loaded
    _broker_ready = table_exists("vw_broker_kpis")

    def _broker_pending():
        st.warning(
            "**Datos de corredores no disponibles aún.**\n\n"
            "Para habilitar esta sección es necesario:\n"
            "1. Descargar los ZIPs XBRL de corredores desde el portal CMF.\n"
            "2. Ejecutar el ETL de corredores (pendiente de desarrollo).\n"
            "3. Los datos se expondrán a través de las vistas `vw_broker_kpis`, "
            "`vw_broker_market_share`, `vw_broker_by_ramo` y `vw_broker_yoy`."
        )

    if page == "Market Overview":
        st.title(f"Corredores — Market Overview — {period_badge()}{seg_badge()}{ramo_badge()}")
        if not _broker_ready:
            _broker_pending()
        else:
            # ── TODO: wire to vw_broker_kpis once ETL is ready ───────────────
            pass

    elif page == "Zurich Focus":
        st.title(f"Corredores — Zurich Focus — {period_badge()}{seg_badge()}{ramo_badge()}")
        if not _broker_ready:
            _broker_pending()
        else:
            pass

    elif page == "Ramo Deep-dive":
        st.title(f"Corredores — Ramo Deep-dive — {period_badge()}{seg_badge()}{ramo_badge()}")
        if not _broker_ready:
            _broker_pending()
        else:
            pass

    elif page == "YoY Trends":
        st.title(f"Corredores — YoY Trends{seg_badge()}{ramo_badge()}")
        if not _broker_ready:
            _broker_pending()
        else:
            pass

    elif page == "M&A Groups":
        st.title(f"Corredores — M&A Groups — {period_badge()}{seg_badge()}")
        if not _broker_ready:
            _broker_pending()
        else:
            pass
