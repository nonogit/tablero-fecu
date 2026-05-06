-- KPI views for Power BI
-- Run once after init_db to create analytical layer.
-- Reads from financials_all (capture-everything raw facts) filtered to
-- is_primary=1 (the dim-free, primary-context numeric values).

-- -----------------------------------------------------------------------
-- Convenience: flattened primary fact table (one row per company/period/concept)
-- -----------------------------------------------------------------------
DROP VIEW IF EXISTS vw_primary_facts;
CREATE VIEW vw_primary_facts AS
SELECT
    fa.company_id,
    fa.period_id,
    fa.concept,
    fa.period_type,
    fa.value_num
FROM financials_all fa
WHERE fa.is_primary = 1
  AND fa.value_num IS NOT NULL;


-- -----------------------------------------------------------------------
-- Company-level KPIs
-- -----------------------------------------------------------------------
DROP VIEW IF EXISTS vw_kpis;
CREATE VIEW vw_kpis AS
WITH base AS (
    SELECT
        c.rut,
        c.name                                              AS company_name,
        p.year,
        p.quarter,
        p.end_date,
        MAX(CASE WHEN f.concept = 'PrimasDirectas'              THEN f.value_num END) AS gwp,
        MAX(CASE WHEN f.concept = 'PrimasCedidas'               THEN f.value_num END) AS ceded_premiums,
        MAX(CASE WHEN f.concept = 'PrimasRetenidas'             THEN f.value_num END) AS net_premiums,
        MAX(CASE WHEN f.concept = 'PrimaDirectaGanada'          THEN f.value_num END) AS gwp_earned,
        MAX(CASE WHEN f.concept = 'PrimaRetenidaGanada'         THEN f.value_num END) AS net_earned_premium,
        MAX(CASE WHEN f.concept = 'CostoSiniestrosDelEjercicio' THEN f.value_num END) AS claims_sinistros,
        MAX(CASE WHEN f.concept = 'CostoDeRentasDelEjercicio'   THEN f.value_num END) AS claims_rentas,
        COALESCE(MAX(CASE WHEN f.concept = 'CostoDeRentasDelEjercicio' THEN f.value_num END), 0)
          + MAX(CASE WHEN f.concept = 'CostoSiniestrosDelEjercicio'    THEN f.value_num END) AS claims_cost,
        MAX(CASE WHEN f.concept = 'SiniestrosPagados'           THEN f.value_num END) AS claims_paid,
        MAX(CASE WHEN f.concept = 'RecuperoSiniestros'          THEN f.value_num END) AS claims_recoveries,
        MAX(CASE WHEN f.concept = 'CostosAdministracion'        THEN f.value_num END) AS admin_costs,
        MAX(CASE WHEN f.concept = 'ResultadoDeIntermediacion'   THEN f.value_num END) AS intermediation_result,
        MAX(CASE WHEN f.concept = 'ResultadoTecnicoDeSeguros'   THEN f.value_num END) AS technical_result,
        MAX(CASE WHEN f.concept = 'ResultadoDeInversiones'      THEN f.value_num END) AS investment_result,
        MAX(CASE WHEN f.concept = 'ResultadoDelEjercicio'       THEN f.value_num END) AS net_result
    FROM vw_primary_facts f
    JOIN companies c ON c.id = f.company_id
    JOIN periods   p ON p.id = f.period_id
    GROUP BY c.rut, p.year, p.quarter
)
SELECT
    rut, company_name, year, quarter, end_date,
    gwp, ceded_premiums, net_premiums, gwp_earned, net_earned_premium,
    claims_sinistros, claims_rentas, claims_cost, claims_paid, claims_recoveries,
    admin_costs, intermediation_result,
    technical_result, investment_result, net_result,
    COALESCE(net_earned_premium, net_premiums)                                AS nep,
    ROUND(100.0 * claims_cost        / NULLIF(COALESCE(net_earned_premium, net_premiums), 0), 2) AS loss_ratio_pct,
    ROUND(100.0 * admin_costs        / NULLIF(COALESCE(net_earned_premium, net_premiums), 0), 2) AS expense_ratio_pct,
    ROUND(100.0 * (claims_cost + admin_costs) / NULLIF(COALESCE(net_earned_premium, net_premiums), 0), 2) AS combined_ratio_pct,
    ROUND(100.0 * ceded_premiums     / NULLIF(gwp, 0), 2)                     AS cession_rate_pct,
    ROUND(100.0 * technical_result   / NULLIF(COALESCE(net_earned_premium, net_premiums), 0), 2) AS technical_margin_pct,
    ROUND(100.0 * net_result         / NULLIF(gwp, 0), 2)                     AS net_margin_pct
FROM base;


DROP VIEW IF EXISTS vw_market_share;
CREATE VIEW vw_market_share AS
SELECT
    k.rut, k.company_name, k.year, k.quarter, k.gwp,
    ROUND(100.0 * k.gwp / SUM(k.gwp) OVER (PARTITION BY k.year, k.quarter), 2) AS market_share_pct
FROM vw_kpis k
WHERE k.gwp IS NOT NULL;


-- Quarter-on-quarter comparison (YTD Dec − YTD Sep, same year)
DROP VIEW IF EXISTS vw_period_qoq;
CREATE VIEW vw_period_qoq AS
SELECT
    curr.*,
    prev.gwp                 AS prev_gwp,
    prev.net_premiums        AS prev_net_premiums,
    prev.loss_ratio_pct      AS prev_loss_ratio_pct,
    prev.combined_ratio_pct  AS prev_combined_ratio_pct,
    prev.net_result          AS prev_net_result,
    ROUND(100.0 * (curr.gwp - prev.gwp) / NULLIF(prev.gwp, 0), 2) AS gwp_qoq_pct
FROM vw_kpis curr
LEFT JOIN vw_kpis prev
    ON  prev.rut     = curr.rut
    AND prev.year    = curr.year - (CASE WHEN curr.quarter = 1 THEN 1 ELSE 0 END)
    AND prev.quarter = CASE WHEN curr.quarter = 1 THEN 4 ELSE curr.quarter - 1 END;


-- Year-on-year comparison (same quarter, prior year) — the right lens for YTD figures
DROP VIEW IF EXISTS vw_period_yoy;
CREATE VIEW vw_period_yoy AS
SELECT
    curr.*,
    prev.gwp                 AS prev_gwp,
    prev.net_premiums        AS prev_net_premiums,
    prev.loss_ratio_pct      AS prev_loss_ratio_pct,
    prev.combined_ratio_pct  AS prev_combined_ratio_pct,
    prev.net_result          AS prev_net_result,
    ROUND(100.0 * (curr.gwp - prev.gwp) / NULLIF(prev.gwp, 0), 2) AS gwp_yoy_pct
FROM vw_kpis curr
LEFT JOIN vw_kpis prev
    ON  prev.rut     = curr.rut
    AND prev.year    = curr.year - 1
    AND prev.quarter = curr.quarter;


-- Backwards-compat alias used by earlier code (QoQ semantics preserved, column renamed)
DROP VIEW IF EXISTS vw_period_comparison;
CREATE VIEW vw_period_comparison AS SELECT * FROM vw_period_qoq;


-- -----------------------------------------------------------------------
-- M&A GROUP VIEWS
-- -----------------------------------------------------------------------

DROP VIEW IF EXISTS vw_group_kpis;
CREATE VIEW vw_group_kpis AS
WITH company_concepts AS (
    SELECT
        f.company_id,
        f.period_id,
        MAX(CASE WHEN f.concept = 'PrimasDirectas'              THEN f.value_num END) AS gwp,
        MAX(CASE WHEN f.concept = 'PrimasCedidas'               THEN f.value_num END) AS ceded_premiums,
        MAX(CASE WHEN f.concept = 'PrimasRetenidas'             THEN f.value_num END) AS net_premiums,
        MAX(CASE WHEN f.concept = 'PrimaRetenidaGanada'         THEN f.value_num END) AS net_earned_premium,
        COALESCE(MAX(CASE WHEN f.concept = 'CostoDeRentasDelEjercicio' THEN f.value_num END), 0)
          + MAX(CASE WHEN f.concept = 'CostoSiniestrosDelEjercicio'    THEN f.value_num END) AS claims_cost,
        MAX(CASE WHEN f.concept = 'CostosAdministracion'        THEN f.value_num END) AS admin_costs,
        MAX(CASE WHEN f.concept = 'ResultadoTecnicoDeSeguros'   THEN f.value_num END) AS technical_result,
        MAX(CASE WHEN f.concept = 'ResultadoDeInversiones'      THEN f.value_num END) AS investment_result,
        MAX(CASE WHEN f.concept = 'ResultadoDelEjercicio'       THEN f.value_num END) AS net_result
    FROM vw_primary_facts f
    GROUP BY f.company_id, f.period_id
),
base AS (
    SELECT
        g.id        AS group_id,
        g.group_name,
        p.year,
        p.quarter,
        p.end_date,
        SUM(cc.gwp)                                            AS gwp,
        SUM(cc.ceded_premiums)                                 AS ceded_premiums,
        SUM(cc.net_premiums)                                   AS net_premiums,
        SUM(COALESCE(cc.net_earned_premium, cc.net_premiums))  AS nep,
        SUM(cc.claims_cost)                                    AS claims_cost,
        SUM(cc.admin_costs)                                    AS admin_costs,
        SUM(cc.technical_result)                               AS technical_result,
        SUM(cc.investment_result)                              AS investment_result,
        SUM(cc.net_result)                                     AS net_result
    FROM company_groups g
    JOIN company_group_members m ON m.group_id = g.id
    JOIN companies c             ON c.id = m.company_id
    JOIN company_concepts cc     ON cc.company_id = c.id
    JOIN periods p               ON p.id = cc.period_id
    WHERE p.end_date >= m.effective_from
      AND (m.effective_to IS NULL OR p.end_date <= m.effective_to)
    GROUP BY g.id, g.group_name, p.year, p.quarter
)
SELECT
    group_id, group_name, year, quarter, end_date,
    gwp, ceded_premiums, net_premiums, nep,
    claims_cost, admin_costs,
    technical_result, investment_result, net_result,
    ROUND(100.0 * claims_cost                 / NULLIF(nep, 0), 2) AS loss_ratio_pct,
    ROUND(100.0 * admin_costs                 / NULLIF(nep, 0), 2) AS expense_ratio_pct,
    ROUND(100.0 * (claims_cost + admin_costs) / NULLIF(nep, 0), 2) AS combined_ratio_pct,
    ROUND(100.0 * net_result                  / NULLIF(gwp, 0), 2) AS net_margin_pct
FROM base;


DROP VIEW IF EXISTS vw_group_market_share;
CREATE VIEW vw_group_market_share AS
SELECT
    group_id, group_name, year, quarter, gwp,
    ROUND(100.0 * gwp / SUM(gwp) OVER (PARTITION BY year, quarter), 2) AS market_share_pct
FROM vw_group_kpis
WHERE gwp > 0;


-- Group-level YoY (same quarter, prior year) — M&A-safe via group membership
DROP VIEW IF EXISTS vw_group_yoy;
CREATE VIEW vw_group_yoy AS
SELECT
    curr.group_id, curr.group_name, curr.year, curr.quarter,
    curr.gwp,
    prev.gwp                             AS prev_gwp,
    curr.loss_ratio_pct,
    prev.loss_ratio_pct                  AS prev_loss_ratio_pct,
    curr.combined_ratio_pct,
    prev.combined_ratio_pct              AS prev_combined_ratio_pct,
    curr.net_result,
    prev.net_result                      AS prev_net_result,
    ROUND(100.0 * (curr.gwp - prev.gwp) / NULLIF(prev.gwp, 0), 2) AS gwp_yoy_pct
FROM vw_group_kpis curr
LEFT JOIN vw_group_kpis prev
    ON  prev.group_id = curr.group_id
    AND prev.year     = curr.year - 1
    AND prev.quarter  = curr.quarter;


-- -----------------------------------------------------------------------
-- RAMO VIEWS  (Power BI — normalized ramo_code layer)
-- -----------------------------------------------------------------------

DROP VIEW IF EXISTS vw_ramo_base;
CREATE VIEW vw_ramo_base AS
SELECT
    c.rut,
    c.name                                      AS company_name,
    c.company_type,
    p.year, p.quarter, p.end_date,
    fr.insurance_type,
    fr.ramo_code,
    d.ramo_name,
    d.product_group,
    fr.concept,
    fr.value
FROM financials_ramo fr
JOIN companies  c ON c.id = fr.company_id
JOIN periods    p ON p.id = fr.period_id
LEFT JOIN dim_ramo d
    ON  d.insurance_type = fr.insurance_type
    AND d.ramo_code      = fr.ramo_code
WHERE d.product_group != 'SUBTOTAL' OR d.product_group IS NULL;


DROP VIEW IF EXISTS vw_gwp_by_product;
CREATE VIEW vw_gwp_by_product AS
SELECT
    rut, company_name, company_type,
    year, quarter, end_date,
    insurance_type, product_group,
    SUM(CASE WHEN concept = 'PrimasDirectas'  THEN value ELSE 0 END) AS gwp,
    SUM(CASE WHEN concept = 'PrimasCedidas'   THEN value ELSE 0 END) AS ceded,
    SUM(CASE WHEN concept = 'PrimasRetenidas' THEN value ELSE 0 END) AS net_premiums
FROM vw_ramo_base
GROUP BY rut, company_name, company_type, year, quarter, end_date,
         insurance_type, product_group;


DROP VIEW IF EXISTS vw_gwp_by_ramo;
CREATE VIEW vw_gwp_by_ramo AS
SELECT
    rut, company_name, company_type,
    year, quarter, end_date,
    insurance_type, ramo_code, ramo_name, product_group,
    SUM(CASE WHEN concept = 'PrimasDirectas'  THEN value ELSE 0 END) AS gwp,
    SUM(CASE WHEN concept = 'PrimasCedidas'   THEN value ELSE 0 END) AS ceded,
    SUM(CASE WHEN concept = 'PrimasRetenidas' THEN value ELSE 0 END) AS net_premiums
FROM vw_ramo_base
GROUP BY rut, company_name, company_type, year, quarter, end_date,
         insurance_type, ramo_code, ramo_name, product_group;


DROP VIEW IF EXISTS vw_market_gwp_by_product;
CREATE VIEW vw_market_gwp_by_product AS
SELECT
    year, quarter, end_date, insurance_type, product_group,
    SUM(gwp) AS market_gwp
FROM vw_gwp_by_product
GROUP BY year, quarter, end_date, insurance_type, product_group;


DROP VIEW IF EXISTS vw_market_gwp_by_ramo;
CREATE VIEW vw_market_gwp_by_ramo AS
SELECT
    year, quarter, end_date, insurance_type, ramo_code, ramo_name, product_group,
    SUM(gwp) AS market_gwp
FROM vw_gwp_by_ramo
GROUP BY year, quarter, end_date, insurance_type, ramo_code, ramo_name, product_group;


-- -----------------------------------------------------------------------
-- Broad-facts helper views (capture-everything)
-- -----------------------------------------------------------------------

-- All primary (dim-free) facts with friendly keys — handy for ad-hoc BI queries
DROP VIEW IF EXISTS vw_facts_primary;
CREATE VIEW vw_facts_primary AS
SELECT
    c.rut, c.name AS company_name, c.company_type,
    p.year, p.quarter, p.end_date,
    fa.concept, fa.period_type, fa.value_num, fa.value_text
FROM financials_all fa
JOIN companies c ON c.id = fa.company_id
JOIN periods   p ON p.id = fa.period_id
WHERE fa.is_primary = 1;


-- All dimensional facts (dim_signature != '') — the raw breakdown layer
DROP VIEW IF EXISTS vw_facts_dim;
CREATE VIEW vw_facts_dim AS
SELECT
    c.rut, c.name AS company_name, c.company_type,
    p.year, p.quarter, p.end_date,
    fa.concept, fa.period_type, fa.dim_signature,
    fa.value_num, fa.value_text
FROM financials_all fa
JOIN companies c ON c.id = fa.company_id
JOIN periods   p ON p.id = fa.period_id
WHERE fa.dim_signature != '';
