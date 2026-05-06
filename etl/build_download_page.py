"""
Generates descarga_trimestral.html inside each quarterly period folder.
Usage:
  python build_download_page.py              # generate for ALL existing YYYYMM folders
  python build_download_page.py --period 202512  # generate for a specific period
"""

import argparse
import json
import re
import sqlite3
import urllib.request
from pathlib import Path

ROOT = Path(__file__).parent.parent
DB   = ROOT / "market.db"
BASE = "https://www.cmfchile.cl/institucional/mercados"
PESTANIA_FIN = 99

QUARTER_LABEL = {1: "Mar", 2: "Jun", 3: "Sep", 4: "Dic"}


def get_last_periods():
    """Return {rut_base: 'Sep 2025 (Q3)'} for every company that has data in the DB."""
    if not DB.exists():
        return {}
    conn = sqlite3.connect(str(DB))
    rows = conn.execute("""
        SELECT c.rut, p.year, p.quarter
        FROM companies c
        JOIN financials f ON f.company_id = c.id
        JOIN periods    p ON p.id = f.period_id
        GROUP BY c.rut
        HAVING p.year * 10 + p.quarter = MAX(p.year * 10 + p.quarter)
    """).fetchall()
    conn.close()
    result = {}
    for rut, year, quarter in rows:
        base = rut.split("-")[0]
        result[base] = f"{QUARTER_LABEL[quarter]} {year} (Q{quarter})"
    return result


def fetch_companies(url, company_type, vig="VI"):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        html = r.read().decode("utf-8", errors="replace")
    rows = re.findall(
        r'rut=(\d+)[^\"]*row=([A-Za-z0-9+/=]+)[^\"]*pestania=1\">[^<]+</a>\s*</td>\s*<td>\s*'
        r'<a href=\"[^\"]*pestania=1\">([^<]+)</a>',
        html,
    )
    seen, results = set(), []
    for rut_base, row, name in rows:
        if rut_base not in seen:
            seen.add(rut_base)
            results.append({"rut_base": rut_base, "row": row,
                            "name": name.strip(), "type": company_type, "vig": vig})
    return results


def make_url(c):
    tipoentidad = "CSGEN" if c["type"] in ("Generales", "Generales NV") else "CSVID"
    return (f"{BASE}/entidad.php?mercado=S&rut={c['rut_base']}&grupo="
            f"&tipoentidad={tipoentidad}"
            f"&vig={c['vig']}&row={c['row']}&control=svs&pestania={PESTANIA_FIN}")


def period_label(period):
    """'202512' -> 'Diciembre 2025 (Q4)'"""
    mm = period[4:6]
    yyyy = period[:4]
    names = {"03": "Marzo (Q1)", "06": "Junio (Q2)",
             "09": "Septiembre (Q3)", "12": "Diciembre (Q4)"}
    return f"{names.get(mm, mm)} {yyyy}"


def build_html(companies, nv_companies, period, last_periods):
    generales = [c for c in companies if c["type"] == "Generales"]
    vida      = [c for c in companies if c["type"] == "Vida"]
    nv_gen    = [c for c in nv_companies if c["type"] == "Generales NV"]
    nv_vida   = [c for c in nv_companies if c["type"] == "Vida NV"]
    label     = period_label(period)
    storage_key = f"cmf_download_progress_{period}"

    def rows(cos, show_last=False):
        html = ""
        for i, c in enumerate(cos, 1):
            url = make_url(c)
            name = c["name"].encode("latin-1", errors="replace").decode("latin-1")
            last = last_periods.get(c["rut_base"], "")
            last_cell = (f'<td class="last-period">{last}</td>' if show_last else "")
            html += f"""
        <tr>
          <td class="num">{i}</td>
          <td class="name">{name}</td>
          <td class="rut">{c['rut_base']}</td>
          <td><a href="{url}" target="_blank" class="btn-open">Abrir</a></td>
          {last_cell}<td><input type="checkbox" class="done-check" onchange="markDone(this)"></td>
        </tr>"""
        return html

    companies_json = json.dumps(
        [{"rut": c["rut_base"],
          "name": c["name"].encode("latin-1", errors="replace").decode("latin-1"),
          "type": c["type"]}
         for c in companies],
        ensure_ascii=False,
    )

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>CMF - Descarga {label}</title>
<style>
  body {{ font-family: Segoe UI, Arial, sans-serif; margin: 0; background: #f4f6f9; color: #1a1a2e; }}
  header {{ background: #1a3c5e; color: white; padding: 18px 32px; }}
  header h1 {{ margin: 0; font-size: 1.4rem; }}
  header p  {{ margin: 4px 0 0; font-size: 0.85rem; opacity: 0.8; }}
  .period-tag {{ display: inline-block; background: #f0a500; color: #1a1a2e; font-weight: 700;
                 border-radius: 4px; padding: 2px 10px; font-size: 0.95rem; margin-left: 10px;
                 vertical-align: middle; }}
  .container {{ max-width: 960px; margin: 28px auto; padding: 0 16px; }}
  .progress-bar {{ background: #dde3ea; border-radius: 8px; height: 14px; margin-bottom: 6px; overflow: hidden; }}
  .progress-fill {{ background: #2a7d4f; height: 100%; width: 0%; transition: width 0.3s; border-radius: 8px; }}
  .progress-label {{ font-size: 0.82rem; color: #555; margin-bottom: 20px; }}
  h2 {{ font-size: 1.1rem; margin: 28px 0 10px; color: #1a3c5e; border-bottom: 2px solid #1a3c5e; padding-bottom: 4px; }}
  table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px;
           box-shadow: 0 1px 4px rgba(0,0,0,.08); overflow: hidden; margin-bottom: 32px; }}
  th {{ background: #1a3c5e; color: white; padding: 9px 12px; text-align: left; font-size: 0.82rem; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #eef0f3; font-size: 0.88rem; vertical-align: middle; }}
  tr:last-child td {{ border-bottom: none; }}
  tr.done {{ background: #edfaf3; }}
  tr.done .name {{ text-decoration: line-through; color: #888; }}
  .num  {{ width: 36px; color: #999; text-align: center; }}
  .rut  {{ width: 110px; color: #666; font-family: monospace; font-size: 0.83rem; }}
  .btn-open {{
    display: inline-block; padding: 4px 14px; background: #1a3c5e; color: white;
    text-decoration: none; border-radius: 4px; font-size: 0.82rem;
  }}
  .btn-open:hover {{ background: #2a5c8e; }}
  .done-check {{ width: 18px; height: 18px; cursor: pointer; }}
  .instructions {{ background: #fff8e1; border-left: 4px solid #f0a500; padding: 12px 16px;
                   border-radius: 4px; font-size: 0.88rem; margin-bottom: 24px; line-height: 1.6; }}
  .instructions ol {{ margin: 6px 0 0 18px; padding: 0; }}
  .reset-btn {{ float: right; padding: 5px 14px; background: #c0392b; color: white;
                border: none; border-radius: 4px; cursor: pointer; font-size: 0.82rem; }}
  .reset-btn:hover {{ background: #e74c3c; }}
  .verify-btn {{ display: block; width: 100%; padding: 13px 20px; margin: 8px 0 24px;
                 background: #2a7d4f; color: white; border: 3px solid #1a5c38;
                 border-radius: 8px; font-size: 1rem; font-weight: 700; cursor: pointer;
                 letter-spacing: 0.02em; box-shadow: 0 3px 8px rgba(0,0,0,.18); text-align: center; }}
  .verify-btn:hover {{ background: #1a5c38; box-shadow: 0 5px 14px rgba(0,0,0,.25); }}
  .verify-btn:active {{ transform: translateY(1px); box-shadow: 0 2px 4px rgba(0,0,0,.15); }}
  .verify-panel {{ border-radius: 6px; padding: 14px 18px; margin-bottom: 24px;
                   font-size: 0.88rem; line-height: 1.7; display: none; }}
  .verify-ok   {{ background: #edfaf3; border-left: 4px solid #2a7d4f; }}
  .verify-warn {{ background: #fff3f3; border-left: 4px solid #c0392b; }}
  .verify-panel .v-title {{ font-weight: 700; font-size: 0.95rem; margin-bottom: 6px; }}
  .verify-panel ul {{ margin: 6px 0 0 18px; padding: 0; }}
  .verify-panel li.miss {{ color: #c0392b; }}
  .verify-panel li.hit  {{ color: #2a7d4f; }}
  .nv-section {{ margin-top: 40px; }}
  .nv-section summary {{
    cursor: pointer; user-select: none; font-size: 1.1rem; font-weight: 700; color: #7a5c1e;
    background: #fdf3dc; border: 1.5px solid #e0c060; border-radius: 6px;
    padding: 10px 16px; list-style: none;
  }}
  .nv-section summary::-webkit-details-marker {{ display: none; }}
  .nv-section summary::before {{ content: "\\25B6\\00A0"; font-size: 0.75rem; }}
  details[open] .nv-section summary::before {{ content: "\\25BC\\00A0"; font-size: 0.75rem; }}
  .nv-note {{ background: #fdf3dc; border-left: 4px solid #e0a000; border-radius: 4px;
              padding: 10px 14px; font-size: 0.85rem; margin: 12px 0 16px; color: #5a4010; }}
  .nv-section h2 {{ color: #7a5c1e; border-bottom-color: #e0c060; }}
  .nv-section th {{ background: #7a5c1e; }}
  .nv-section .btn-open {{ background: #7a5c1e; }}
  .nv-section .btn-open:hover {{ background: #5a3c0e; }}
  .last-period {{ width: 130px; font-size: 0.82rem; color: #7a5c1e; font-style: italic; }}
</style>
</head>
<body>
<header>
  <h1>CMF — Descarga de Archivos XBRL <span class="period-tag">{label}</span></h1>
  <p>{len(companies)} companias vigentes ({len(generales)} Generales + {len(vida)} Vida) &bull; {len(nv_companies)} no vigentes &bull; Carpeta: {period}/descargas/</p>
</header>
<div class="container">

  <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
  <div class="progress-label" id="progressLabel">0 / {len(companies)} completadas</div>

  <div class="instructions">
    <strong>Instrucciones de descarga trimestral:</strong>
    <ol>
      <li><strong>Por cada compania en las tablas de abajo:</strong>
        <ol type="a">
          <li>Haz clic en <strong>Abrir</strong> — se abrira la pestana <em>Informacion Financiera</em> en CMF</li>
          <li>Selecciona el trimestre <strong>{label}</strong> en los combos de periodo (mes y ano)</li>
          <li>Resuelve el captcha y descarga el archivo <code>.zip</code></li>
          <li>Mueve el <code>.zip</code> descargado a la carpeta <code>{period}/descargas/</code></li>
          <li>Marca el checkbox <strong>Listo</strong> en esta pagina</li>
        </ol>
      </li>
      <li><strong>Una vez marcadas todas las companias:</strong> haz clic en el boton verde de abajo, selecciona la carpeta <code>{period}/descargas/</code> y verifica que todos los archivos esten presentes.</li>
      <li>Si hay faltantes, repite el paso 1 para las companias indicadas.</li>
      <li>Cuando la verificacion sea exitosa, ejecuta <code>python etl/run_quarter.py --period {period}</code> para cargar los datos.</li>
    </ol>
  </div>
  <button class="reset-btn" onclick="resetAll()">Resetear progreso</button>

  <button class="verify-btn" onclick="verifyDownloads()">&#10003; Una vez completadas todas las descargas, verificar las descargas completadas</button>
  <div class="verify-panel" id="verifyPanel"></div>

  <h2>Seguros Generales ({len(generales)} companias)</h2>
  <table>
    <thead><tr><th>#</th><th>Compania</th><th>RUT base</th><th>Portal CMF</th><th>Listo</th></tr></thead>
    <tbody>{rows(generales)}</tbody>
  </table>

  <h2>Seguros de Vida ({len(vida)} companias)</h2>
  <table>
    <thead><tr><th>#</th><th>Compania</th><th>RUT base</th><th>Portal CMF</th><th>Listo</th></tr></thead>
    <tbody>{rows(vida)}</tbody>
  </table>

  <details class="nv-section">
    <summary>Companias No Vigentes ({len(nv_companies)} — {len(nv_gen)} Generales + {len(nv_vida)} Vida)</summary>
    <div class="nv-note">
      Estas companias ya no estan activas en CMF. Usar solo para descargar datos historicos de periodos en que aun operaban.
      La columna <em>Ultimo dato en BD</em> indica el ultimo trimestre registrado en la base de datos local.
      El progreso de estas companias no cuenta en la barra superior.
    </div>
    <h2>Generales No Vigentes ({len(nv_gen)} companias)</h2>
    <table>
      <thead><tr><th>#</th><th>Compania</th><th>RUT base</th><th>Portal CMF</th><th>Ultimo dato en BD</th><th>Listo</th></tr></thead>
      <tbody>{rows(nv_gen, show_last=True)}</tbody>
    </table>
    <h2>Vida No Vigentes ({len(nv_vida)} companias)</h2>
    <table>
      <thead><tr><th>#</th><th>Compania</th><th>RUT base</th><th>Portal CMF</th><th>Ultimo dato en BD</th><th>Listo</th></tr></thead>
      <tbody>{rows(nv_vida, show_last=True)}</tbody>
    </table>
  </details>

</div>
<script>
  const STORAGE_KEY = '{storage_key}';
  const total = {len(companies)};
  const COMPANIES = {companies_json};

  async function verifyDownloads() {{
    const panel = document.getElementById('verifyPanel');
    if (!window.showDirectoryPicker) {{
      panel.className = 'verify-panel verify-warn';
      panel.style.display = 'block';
      panel.innerHTML = '<div class="v-title">Funcion no disponible</div>'
        + 'Tu navegador no soporta acceso al sistema de archivos. Usa Chrome o Edge.';
      return;
    }}
    let dirHandle;
    try {{
      dirHandle = await window.showDirectoryPicker({{ mode: 'read' }});
    }} catch (e) {{ return; }}

    const zips = [];
    for await (const entry of dirHandle.values()) {{
      if (entry.kind === 'file' && entry.name.toLowerCase().endsWith('.zip'))
        zips.push(entry.name.toLowerCase());
    }}

    const found = [], missing = [];
    for (const c of COMPANIES) {{
      const hit = zips.some(f => f.startsWith(c.rut.toLowerCase()));
      (hit ? found : missing).push(c);
    }}

    const ok = missing.length === 0;
    panel.className = 'verify-panel ' + (ok ? 'verify-ok' : 'verify-warn');
    panel.style.display = 'block';

    let html = '<div class="v-title">'
      + (ok ? 'Todas las descargas completas (' + found.length + '/' + total + ')'
            : missing.length + ' archivo(s) faltante(s) — ' + found.length + '/' + total + ' encontrados')
      + '</div>';
    if (!ok) {{
      html += '<ul>';
      for (const c of missing)
        html += '<li class="miss">' + c.name + ' <span style="color:#999;font-size:0.8em">(' + c.type + ' — RUT ' + c.rut + ')</span></li>';
      html += '</ul>';
    }}
    if (found.length > 0) {{
      html += '<details style="margin-top:8px"><summary style="cursor:pointer;color:#555;font-size:0.83rem">Ver archivos encontrados (' + found.length + ')</summary><ul>';
      for (const c of found)
        html += '<li class="hit">' + c.name + '</li>';
      html += '</ul></details>';
    }}
    panel.innerHTML = html;
  }}

  function getState() {{
    try {{ return JSON.parse(localStorage.getItem(STORAGE_KEY) || '{{}}'); }}
    catch {{ return {{}}; }}
  }}
  function saveState(s) {{ localStorage.setItem(STORAGE_KEY, JSON.stringify(s)); }}

  function updateProgress() {{
    const checks = document.querySelectorAll('.done-check');
    const done = Array.from(checks).filter(c => c.checked).length;
    document.getElementById('progressFill').style.width = (done / total * 100) + '%';
    document.getElementById('progressLabel').textContent = done + ' / ' + total + ' completadas';
  }}

  function markDone(checkbox) {{
    const row = checkbox.closest('tr');
    const name = row.querySelector('.name').textContent;
    row.classList.toggle('done', checkbox.checked);
    const s = getState();
    if (checkbox.checked) s[name] = true; else delete s[name];
    saveState(s);
    updateProgress();
  }}

  function resetAll() {{
    if (!confirm('Resetear todo el progreso de {label}?')) return;
    localStorage.removeItem(STORAGE_KEY);
    document.querySelectorAll('.done-check').forEach(c => {{
      c.checked = false;
      c.closest('tr').classList.remove('done');
    }});
    updateProgress();
  }}

  (function() {{
    const s = getState();
    document.querySelectorAll('tr').forEach(row => {{
      const nameEl = row.querySelector('.name');
      const check  = row.querySelector('.done-check');
      if (nameEl && check && s[nameEl.textContent]) {{
        check.checked = true;
        row.classList.add('done');
      }}
    }});
    updateProgress();
  }})();
</script>
</body>
</html>"""


def find_period_folders():
    return sorted(
        p.name for p in ROOT.iterdir()
        if p.is_dir() and re.fullmatch(r"\d{6}", p.name)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", help="Single period to generate, e.g. 202512")
    args = parser.parse_args()

    periods = [args.period] if args.period else find_period_folders()
    if not periods:
        print("No period folders found.")
        raise SystemExit(1)

    print("Fetching company lists from CMF (done once for all periods)...")
    gen  = fetch_companies(
        "https://www.cmfchile.cl/institucional/mercados/consulta.php?mercado=S&Estado=VI&consulta=CSGEN",
        "Generales", vig="VI",
    )
    life = fetch_companies(
        "https://www.cmfchile.cl/institucional/mercados/consulta.php?mercado=S&Estado=VI&consulta=CSVID",
        "Vida", vig="VI",
    )
    nv_gen  = fetch_companies(
        "https://www.cmfchile.cl/institucional/mercados/consulta.php?mercado=S&Estado=NV&consulta=CSGEN",
        "Generales NV", vig="NV",
    )
    nv_life = fetch_companies(
        "https://www.cmfchile.cl/institucional/mercados/consulta.php?mercado=S&Estado=NV&consulta=CSVID",
        "Vida NV", vig="NV",
    )
    companies    = gen + life
    nv_companies = nv_gen + nv_life
    print(f"  {len(gen)} Generales + {len(life)} Vida = {len(companies)} active")
    print(f"  {len(nv_gen)} Generales NV + {len(nv_life)} Vida NV = {len(nv_companies)} NV")
    print()

    last_periods = get_last_periods()
    print(f"  Last-period data found for {len(last_periods)} companies in DB")
    print()

    for period in periods:
        out = ROOT / period / "descarga_trimestral.html"
        html = build_html(companies, nv_companies, period, last_periods)
        out.write_text(html, encoding="utf-8")
        print(f"Generated: {out}")
