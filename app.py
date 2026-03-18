# app.py
import io
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from config import DATE_FORMATS, PARQUET_MESTRE

# ── Página ───────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Detector de Duplicidades",
    page_icon="🔍",
    layout="wide",
)

# ── DuckDB helpers ────────────────────────────────────────────────────────────
def get_con():
    """Conexão DuckDB em memória (stateless por request)."""
    return duckdb.connect()


@st.cache_data(show_spinner="⚡ Lendo base histórica via DuckDB...")
def carregar_base(
    data_ini: str | None = None,
    data_fim: str | None = None,
    lotes: list[str] | None = None,
) -> pd.DataFrame:
    """
    Lê o Parquet mestre via DuckDB, já filtrando na leitura.
    Retorna apenas as colunas necessárias para o app.
    """
    if not PARQUET_MESTRE.exists():
        return pd.DataFrame()

    con    = get_con()
    where  = []

    if data_ini:
        where.append(f"_data_parsed >= DATE '{data_ini}'")
    if data_fim:
        where.append(f"_data_parsed <= DATE '{data_fim}'")
    if lotes:
        lotes_str = ", ".join(f"'{l}'" for l in lotes)
        where.append(f"_lote IN ({lotes_str})")

    where_clause = f"WHERE {' AND '.join(where)}" if where else ""

    query = f"""
        SELECT *
        FROM read_parquet('{PARQUET_MESTRE}')
        {where_clause}
        ORDER BY _data_parsed
    """

    df = con.execute(query).df()
    con.close()
    return df


@st.cache_data(show_spinner="📋 Carregando lotes disponíveis...")
def listar_lotes() -> list[str]:
    if not PARQUET_MESTRE.exists():
        return []
    con   = get_con()
    query = f"""
        SELECT DISTINCT _lote
        FROM read_parquet('{PARQUET_MESTRE}')
        ORDER BY _lote
    """
    lotes = con.execute(query).df()["_lote"].tolist()
    con.close()
    return lotes


@st.cache_data(show_spinner="📊 Calculando estatísticas da base...")
def stats_base() -> dict:
    if not PARQUET_MESTRE.exists():
        return {}
    con   = get_con()
    query = f"""
        SELECT
            COUNT(*)                        AS total_registros,
            COUNT(DISTINCT _lote)           AS total_lotes,
            MIN(_data_parsed)::VARCHAR      AS data_min,
            MAX(_data_parsed)::VARCHAR      AS data_max,
            COUNT(DISTINCT _cliente_norm)   AS clientes_unicos,
            COUNT(DISTINCT _servico_norm)   AS servicos_unicos
        FROM read_parquet('{PARQUET_MESTRE}')
    """
    row = con.execute(query).df().iloc[0].to_dict()
    con.close()
    return row


# ── Parse de datas ────────────────────────────────────────────────────────────
def parse_dates_robust(series: pd.Series) -> pd.Series:
    result    = pd.Series([pd.NaT] * len(series), index=series.index)
    remaining = series.copy()

    for fmt in DATE_FORMATS:
        mask = result.isna() & remaining.notna()
        if not mask.any():
            break
        parsed = pd.to_datetime(remaining[mask], format=fmt, errors="coerce")
        result[mask] = parsed

    still_null = result.isna() & series.notna()
    if still_null.any():
        parsed_fallback = pd.to_datetime(
            series[still_null], infer_datetime_format=True, errors="coerce"
        )
        result[still_null] = parsed_fallback

    return result


# ── Detecção de duplicidades ──────────────────────────────────────────────────
@st.cache_data(show_spinner="🔎 Detectando duplicidades...")
def detect_duplicates(
    df: pd.DataFrame,
    col_cliente: str,
    col_servico: str,
    col_data: str,
    janela_dias: int,
    col_os: str | None = None,
    cols_extras: tuple[str, ...] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    work = df.copy()

    # Se _data_parsed já existe (veio do Parquet), usa direto
    if "_data_parsed" not in work.columns:
        work["_data_parsed"] = parse_dates_robust(work[col_data].astype(str).str.strip())

    work["_data_parsed"]  = pd.to_datetime(work["_data_parsed"], errors="coerce")
    work["_cliente_norm"] = work[col_cliente].astype(str).str.strip().str.upper()
    work["_servico_norm"] = work[col_servico].astype(str).str.strip().str.upper()

    total     = len(work)
    invalidas = work["_data_parsed"].isna().sum()
    validas   = total - invalidas

    with st.expander("📅 Diagnóstico de datas", expanded=invalidas > 0):
        d1, d2, d3 = st.columns(3)
        d1.metric("Total de registros",       f"{total:,}")
        d2.metric("Datas reconhecidas",        f"{validas:,}")
        d3.metric("Datas inválidas/ignoradas", f"{invalidas:,}",
                  delta=f"-{invalidas}" if invalidas else None,
                  delta_color="inverse")

        if invalidas > 0:
            exemplos = work[work["_data_parsed"].isna()][col_data].dropna().unique()[:10]
            st.warning(f"Exemplos: `{'`, `'.join(map(str, exemplos))}`")
        else:
            amostra = work[[col_data, "_data_parsed"]].dropna().head(5).copy()
            amostra["_data_parsed"] = amostra["_data_parsed"].dt.strftime("%d/%m/%Y")
            amostra.columns = ["Valor original", "Interpretado como"]
            st.success("Todas as datas foram reconhecidas.")
            st.dataframe(amostra, use_container_width=True)

    work = work.dropna(subset=["_data_parsed"]).sort_values("_data_parsed").reset_index(drop=True)
    work["_row_id"] = work.index

    janela      = pd.Timedelta(days=janela_dias)
    resultados  = []
    seen_indices = set()

    for (cliente, servico), grp in work.groupby(["_cliente_norm", "_servico_norm"]):
        if len(grp) < 2:
            continue

        grp     = grp.reset_index(drop=True)
        datas   = grp["_data_parsed"].tolist()
        row_ids = grp["_row_id"].tolist()

        grupo_counter = 0
        i = 0
        while i < len(datas):
            limite         = datas[i] + janela
            cluster_local  = [j for j in range(i, len(datas)) if datas[j] <= limite]

            if len(cluster_local) > 1:
                grupo_counter += 1
                for pos, j in enumerate(cluster_local):
                    rid = row_ids[j]
                    if rid not in seen_indices:
                        seen_indices.add(rid)
                        resultados.append({
                            "_row_id":           rid,
                            "grupo_duplicidade":  grupo_counter,
                            "tipo_registro":     "ORIGINAL" if pos == 0 else "DUPLICATA",
                        })
                i = cluster_local[-1] + 1
            else:
                i += 1

    if not resultados:
        return pd.DataFrame(), pd.DataFrame()

    df_result = pd.DataFrame(resultados).drop_duplicates("_row_id")
    df_merged = work.merge(df_result, on="_row_id", how="inner")
    df_merged = df_merged.sort_values(["grupo_duplicidade", "_data_parsed"])

    output_cols = ["grupo_duplicidade", "tipo_registro"]
    if col_os:
        output_cols.append(col_os)
    output_cols += [col_cliente, col_servico, col_data]
    if cols_extras:
        output_cols += [c for c in cols_extras if c not in output_cols]

    df_dup_out = df_merged[[c for c in output_cols if c in df_merged.columns]].copy()
    df_dup_out[col_data] = df_merged["_data_parsed"].dt.strftime("%d/%m/%Y")

    resumo_rows = []
    for gid, grp in df_merged.groupby("grupo_duplicidade"):
        grp_orig = grp[grp["tipo_registro"] == "ORIGINAL"]
        grp_dup  = grp[grp["tipo_registro"] == "DUPLICATA"]
        if grp_orig.empty:
            continue
        original  = grp_orig.iloc[0]
        datas_dup = grp_dup["_data_parsed"]
        row = {
            "grupo":                     int(gid),
            "cliente":                   original[col_cliente],
            "tipo_servico":              original[col_servico],
            "data_os_original":          original["_data_parsed"].strftime("%d/%m/%Y"),
            "qtd_duplicatas":            len(grp_dup),
            "data_primeira_duplicata":   datas_dup.min().strftime("%d/%m/%Y") if not grp_dup.empty else "—",
            "data_ultima_duplicata":     datas_dup.max().strftime("%d/%m/%Y") if not grp_dup.empty else "—",
            "intervalo_dias_duplicatas": (datas_dup.max() - datas_dup.min()).days if not grp_dup.empty else 0,
        }
        if col_os:
            row["os_original"]   = str(original[col_os])
            row["os_duplicadas"] = ", ".join(grp_dup[col_os].astype(str).tolist())
        resumo_rows.append(row)

    return df_dup_out, pd.DataFrame(resumo_rows)


# ── Exportação Excel ──────────────────────────────────────────────────────────
def to_excel_bytes(df_dup: pd.DataFrame, df_resumo: pd.DataFrame, janela_dias: int) -> bytes:
    wb = Workbook()

    header_fill  = PatternFill("solid", fgColor="1F4E79")
    header_font  = Font(bold=True, color="FFFFFF", size=11)
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin         = Side(style="thin", color="BFBFBF")
    border       = Border(left=thin, right=thin, top=thin, bottom=thin)
    palette      = ["FFF2CC", "FDEBD0", "D5F5E3", "D6EAF8", "F9EBEA",
                    "EAF2FF", "FDF2F8", "E8F8F5", "FDFEFE", "F4ECF7"]
    fill_original = PatternFill("solid", fgColor="C6EFCE")

    def write_sheet(ws, df, title, grupo_col=None):
        ws.title = title
        if df.empty:
            ws.cell(1, 1, "Nenhum dado para exibir.")
            return

        for c_idx, col in enumerate(df.columns, 1):
            cell           = ws.cell(row=1, column=c_idx, value=col)
            cell.fill      = header_fill
            cell.font      = header_font
            cell.alignment = header_align
            cell.border    = border
            ws.column_dimensions[get_column_letter(c_idx)].width = max(16, len(str(col)) + 6)
        ws.row_dimensions[1].height = 28

        has_tipo = "tipo_registro" in df.columns

        for r_idx, row in enumerate(df.itertuples(index=False), 2):
            grupo_val = None
            if grupo_col and grupo_col in df.columns:
                grupo_val = getattr(row, grupo_col.replace(" ", "_"), None)

            tipo_val = getattr(row, "tipo_registro", None) if has_tipo else None

            if tipo_val == "ORIGINAL":
                row_fill = fill_original
            elif grupo_val is not None:
                color    = palette[(int(grupo_val) - 1) % len(palette)]
                row_fill = PatternFill("solid", fgColor=color)
            else:
                row_fill = None

            for c_idx, val in enumerate(row, 1):
                cell           = ws.cell(row=r_idx, column=c_idx, value=val if pd.notna(val) else None)
                cell.border    = border
                cell.alignment = Alignment(vertical="center")
                if row_fill:
                    cell.fill = row_fill

        ws.freeze_panes = "A2"

    ws1 = wb.active
    write_sheet(ws1, df_dup, "Duplicidades", grupo_col="grupo_duplicidade")

    ws2 = wb.create_sheet("Resumo por Grupo")
    write_sheet(ws2, df_resumo, "Resumo por Grupo", grupo_col="grupo")

    ws3 = wb.create_sheet("Configurações")
    ws3.column_dimensions["A"].width = 30
    ws3.column_dimensions["B"].width = 30
    meta = [
        ("Gerado em",                datetime.now().strftime("%d/%m/%Y %H:%M")),
        ("Janela de análise (dias)", janela_dias),
        ("Total de grupos",          df_resumo["grupo"].nunique() if not df_resumo.empty else 0),
        ("Total de OS duplicadas",   int(df_dup[df_dup["tipo_registro"] == "DUPLICATA"].shape[0])
                                     if not df_dup.empty and "tipo_registro" in df_dup.columns else 0),
    ]
    for r, (k, v) in enumerate(meta, 1):
        ws3.cell(r, 1, k).font = Font(bold=True)
        ws3.cell(r, 2, str(v))

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


# ── Paginação ─────────────────────────────────────────────────────────────────
def paginar(df: pd.DataFrame, key: str, page_size: int = 500):
    if len(df) <= page_size:
        st.dataframe(df, use_container_width=True, height=450)
        return

    total_pages = (len(df) - 1) // page_size + 1
    page = st.number_input(
        "Página", min_value=1, max_value=total_pages, value=1, step=1, key=key
    )
    start = (page - 1) * page_size
    st.dataframe(df.iloc[start: start + page_size], use_container_width=True, height=450)
    st.caption(f"Exibindo {start+1:,}–{min(start+page_size, len(df)):,} de {len(df):,} registros")


# ── Interface ────────────────────────────────────────────────────────────────
st.title("🔍 Detector de Duplicidades de OS")
st.caption("Base histórica via DuckDB + Parquet | A OS original não entra na contagem de duplicatas.")

# ── Sidebar: status da base mestre ───────────────────────────────────────────
with st.sidebar:
    st.header("📦 Base Histórica")

    if not PARQUET_MESTRE.exists():
        st.warning("Nenhuma base mestre encontrada.\nExecute `atualiza_base.py` para criar.")
    else:
        stats = stats_base()
        st.metric("Total de OS",        f"{int(stats.get('total_registros', 0)):,}")
        st.metric("Lotes ingeridos",     stats.get("total_lotes", 0))
        st.metric("Clientes únicos",     f"{int(stats.get('clientes_unicos', 0)):,}")
        st.metric("Serviços distintos",  stats.get("servicos_unicos", 0))

        data_min = stats.get("data_min", "")
        data_max = stats.get("data_max", "")
        if data_min and data_max:
            try:
                d_min = pd.to_datetime(data_min).strftime("%d/%m/%Y")
                d_max = pd.to_datetime(data_max).strftime("%d/%m/%Y")
                st.caption(f"Período: {d_min} → {d_max}")
            except Exception:
                pass

        if st.button("🔄 Recarregar estatísticas", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
# Adicione no app.py, antes do st.divider() principal
with st.sidebar:
    st.divider()
    st.subheader("📥 Ingerir novo lote")

    arq_lote = st.file_uploader(
        "Arquivo do lote (.xlsx/.csv)",
        type=["xlsx", "xls", "csv"],
        key="upload_lote"
    )

    if arq_lote:
        cols_lote = None

        # Lê só o cabeçalho para mostrar as colunas
        buf = arq_lote.read()
        import io as _io
        df_preview = (
            pd.read_csv(_io.BytesIO(buf), dtype=str, nrows=3)
            if arq_lote.name.endswith(".csv")
            else pd.read_excel(_io.BytesIO(buf), dtype=str, nrows=3)
        )
        df_preview.columns = df_preview.columns.str.strip()
        cols_lote = list(df_preview.columns)

        lote_nome    = st.text_input("Nome do lote", value=datetime.now().strftime("%Y-%m"))
        lote_cliente = st.selectbox("Coluna cliente",  cols_lote, key="lc")
        lote_servico = st.selectbox("Coluna serviço",  cols_lote, key="ls")
        lote_data    = st.selectbox("Coluna data",     cols_lote, key="ld")
        lote_os      = st.selectbox("Coluna OS (opcional)", ["— nenhuma —"] + cols_lote, key="lo")
        lote_os      = None if lote_os == "— nenhuma —" else lote_os

        if st.button("⚙️ Processar e adicionar à base", use_container_width=True):
            import io as _io2
            df_lote = (
                pd.read_csv(_io2.BytesIO(buf), dtype=str)
                if arq_lote.name.endswith(".csv")
                else pd.read_excel(_io2.BytesIO(buf), dtype=str)
            )
            df_lote.columns = df_lote.columns.str.strip()

            df_lote["_data_parsed"]  = parse_dates_robust(df_lote[lote_data].astype(str).str.strip())
            df_lote["_cliente_norm"] = df_lote[lote_cliente].astype(str).str.strip().str.upper()
            df_lote["_servico_norm"] = df_lote[lote_servico].astype(str).str.strip().str.upper()
            df_lote["_lote"]         = lote_nome
            df_lote["_ingestao_ts"]  = datetime.now().isoformat()

            # Verifica lote duplicado
            lotes_existentes = listar_lotes()
            if lote_nome in lotes_existentes:
                st.error(f"Lote '{lote_nome}' já existe na base. Escolha outro nome.")
            else:
                # Atualiza mestre
                if PARQUET_MESTRE.exists():
                    df_antigo = pd.read_parquet(PARQUET_MESTRE)
                    df_total  = pd.concat([df_antigo, df_lote], ignore_index=True)
                else:
                    df_total = df_lote

                df_total.to_parquet(PARQUET_MESTRE, index=False, compression="snappy")

                invalidas = df_lote["_data_parsed"].isna().sum()
                st.success(
                    f"✅ Lote **{lote_nome}** adicionado!\n\n"
                    f"- {len(df_lote):,} registros no lote\n"
                    f"- {len(df_total):,} registros na base total\n"
                    f"- {invalidas:,} datas inválidas ignoradas"
                )
                st.cache_data.clear()
                st.rerun()
    st.divider()
    st.caption("Para ingerir um novo lote:\n```\npython atualiza_base.py \\\n  --arquivo lote.xlsx \\\n  --col_cliente matricula \\\n  --col_servico tipo_servico \\\n  --col_data data_abertura\n```")

st.divider()

# ── Verifica se base existe ───────────────────────────────────────────────────
if not PARQUET_MESTRE.exists():
    st.error("Base histórica não encontrada. Execute `atualiza_base.py` antes de usar o app.")
    st.stop()

# ── Configurações do filtro de consulta ───────────────────────────────────────
st.subheader("🗓️ Filtros de consulta")

lotes_disponiveis = listar_lotes()

fc1, fc2, fc3 = st.columns(3)

usar_periodo = fc1.checkbox("Filtrar por período de datas", value=True)
usar_lotes   = fc2.checkbox("Filtrar por lote(s) específico(s)")

data_ini_q = data_fim_q = None
lotes_sel  = None

if usar_periodo:
    p1, p2 = st.columns(2)
    data_ini_q = str(p1.date_input("Data inicial", value=datetime.today() - timedelta(days=90)))
    data_fim_q = str(p2.date_input("Data final",   value=datetime.today()))

if usar_lotes:
    lotes_sel = st.multiselect("Lotes", lotes_disponiveis, default=lotes_disponiveis[-1:] if lotes_disponiveis else [])

if st.button("📥 Carregar dados da base", use_container_width=True):
    df_base = carregar_base(
        data_ini=data_ini_q,
        data_fim=data_fim_q,
        lotes=lotes_sel if usar_lotes else None,
    )
    st.session_state["df_base"] = df_base
    st.success(f"{len(df_base):,} registros carregados.")

# ── Configurações de análise ──────────────────────────────────────────────────
if "df_base" in st.session_state and not st.session_state["df_base"].empty:
    df_base = st.session_state["df_base"]

    with st.expander("👁️ Prévia dos dados carregados (10 primeiras linhas)", expanded=False):
        st.dataframe(df_base.head(10), use_container_width=True)

    st.divider()
    st.subheader("⚙️ Configurações da análise")

    # Remove colunas internas do Parquet das opções de seleção
    cols_visiveis = [c for c in df_base.columns if not c.startswith("_")]

    def suggest(keywords, cols):
        for kw in keywords:
            for col in cols:
                if kw.lower() in col.lower():
                    return col
        return cols[0] if cols else None

    ca1, ca2, ca3 = st.columns(3)

    col_cliente = ca1.selectbox(
        "👤 Coluna de cliente",
        cols_visiveis,
        index=cols_visiveis.index(suggest(["cliente", "matricula", "cpf", "cod"], cols_visiveis) or cols_visiveis[0]),
    )
    col_servico = ca2.selectbox(
        "🔧 Coluna de tipo de serviço",
        cols_visiveis,
        index=cols_visiveis.index(suggest(["servico", "serviço", "tipo", "categoria"], cols_visiveis) or cols_visiveis[0]),
    )
    col_data = ca3.selectbox(
        "📅 Coluna de data",
        cols_visiveis,
        index=cols_visiveis.index(suggest(["data", "dt_", "abertura", "criacao"], cols_visiveis) or cols_visiveis[0]),
    )

    ca4, ca5 = st.columns(2)
    none_opt = "— nenhuma —"

    col_os = ca4.selectbox(
        "🔢 Coluna de número da OS (opcional)",
        [none_opt] + cols_visiveis,
    )
    col_os = None if col_os == none_opt else col_os

    janela_dias = ca5.number_input(
        "📆 Janela de duplicidade (dias)",
        min_value=1, max_value=3650, value=30, step=1,
    )

    extras_disp = [c for c in cols_visiveis if c not in [col_cliente, col_servico, col_data, col_os]]
    cols_extras = st.multiselect("➕ Colunas extras no relatório (opcional)", extras_disp)

    st.divider()

    if st.button("🚀 Analisar duplicidades", type="primary", use_container_width=True):
        df_dup, df_resumo = detect_duplicates(
            df=df_base,
            col_cliente=col_cliente,
            col_servico=col_servico,
            col_data=col_data,
            janela_dias=int(janela_dias),
            col_os=col_os,
            cols_extras=tuple(cols_extras) if cols_extras else None,
        )
        st.session_state["df_dup"]    = df_dup
        st.session_state["df_resumo"] = df_resumo
        st.session_state["janela"]    = int(janela_dias)

    # ── Resultados ────────────────────────────────────────────────────────────
    if "df_dup" in st.session_state:
        df_dup    = st.session_state["df_dup"]
        df_resumo = st.session_state["df_resumo"]
        janela    = st.session_state["janela"]

        st.divider()

        if df_dup.empty:
            st.success("✅ Nenhuma duplicidade encontrada com os parâmetros informados.")
        else:
            qtd_dup = int(df_dup[df_dup["tipo_registro"] == "DUPLICATA"].shape[0]) if not df_dup.empty else 0

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Grupos duplicados",      df_resumo["grupo"].nunique())
            k2.metric("Total de OS duplicadas", f"{qtd_dup:,}")
            k3.metric("Janela utilizada",        f"{janela} dias")
            k4.metric("Serviços únicos dup.",    df_resumo["tipo_servico"].nunique())

            st.divider()

            tab1, tab2 = st.tabs(["📄 Detalhamento", "📊 Resumo por Grupo"])

            with tab1:
                st.caption("🟢 Verde = OS original (legítima) | 🟡 Colorido por grupo = OS duplicata")
                paginar(df_dup, key="pag_det")

            with tab2:
                st.caption("Uma linha por grupo — mostra a OS original e quantas duplicatas foram abertas.")
                paginar(df_resumo, key="pag_res")

            st.divider()

            xlsx_bytes = to_excel_bytes(df_dup, df_resumo, janela)
            st.download_button(
                label="⬇️ Baixar relatório XLSX",
                data=xlsx_bytes,
                file_name=f"duplicidades_os_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
