import streamlit as st
import pandas as pd
import io
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── Configuração da página ──────────────────────────────────────────────────
st.set_page_config(
    page_title="Detector de Duplicidades",
    page_icon="🔍",
    layout="wide",
)

# ── Helpers ─────────────────────────────────────────────────────────────────
def parse_dates_robust(series: pd.Series) -> pd.Series:
    """
    Tenta converter datas testando múltiplos formatos explicitamente.
    Evita ambiguidade do dayfirst=True que falha para dias > 12.
    """
    formats = [
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%d.%m.%Y",
    ]

    result = pd.Series([pd.NaT] * len(series), index=series.index)
    remaining = series.copy()

    for fmt in formats:
        mask = result.isna() & remaining.notna()
        if not mask.any():
            break
        parsed = pd.to_datetime(remaining[mask], format=fmt, errors="coerce")
        result[mask] = parsed

    # Fallback genérico para formatos não cobertos
    still_null = result.isna() & series.notna()
    if still_null.any():
        parsed_fallback = pd.to_datetime(series[still_null], infer_datetime_format=True, errors="coerce")
        result[still_null] = parsed_fallback

    return result


def load_file(uploaded_file) -> pd.DataFrame | None:
    name = uploaded_file.name.lower()
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(uploaded_file, dtype=str)
        elif name.endswith((".xlsx", ".xls")):
            df = pd.read_excel(uploaded_file, dtype=str)
        else:
            st.error("Formato não suportado. Envie um arquivo .xlsx, .xls ou .csv.")
            return None
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")
        return None


def detect_duplicates(
    df: pd.DataFrame,
    col_cliente: str,
    col_servico: str,
    col_data: str,
    janela_dias: int,
    col_os: str | None = None,
    cols_extras: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    work = df.copy()

    # Parsing robusto de datas
    work["_data_parsed"] = parse_dates_robust(work[col_data].astype(str).str.strip())

    total     = len(work)
    invalidas = work["_data_parsed"].isna().sum()
    validas   = total - invalidas

    with st.expander("📅 Diagnóstico de parsing de datas", expanded=invalidas > 0):
        d1, d2, d3 = st.columns(3)
        d1.metric("Total de registros",       total)
        d2.metric("Datas reconhecidas",        validas)
        d3.metric("Datas inválidas/ignoradas", invalidas,
                  delta=f"-{invalidas}" if invalidas else None,
                  delta_color="inverse")

        if invalidas > 0:
            exemplos = work[work["_data_parsed"].isna()][col_data].dropna().unique()[:10]
            st.warning(f"Exemplos de valores não reconhecidos: `{'`, `'.join(map(str, exemplos))}`")
        else:
            amostra = work[[col_data, "_data_parsed"]].dropna().head(5).copy()
            amostra["_data_parsed"] = amostra["_data_parsed"].dt.strftime("%d/%m/%Y")
            amostra.columns = ["Valor original", "Interpretado como"]
            st.success("Todas as datas foram reconhecidas com sucesso.")
            st.dataframe(amostra, use_container_width=True)

    work = work.dropna(subset=["_data_parsed"]).copy()
    work = work.sort_values("_data_parsed").reset_index(drop=True)

    work["_cliente_norm"] = work[col_cliente].astype(str).str.strip().str.upper()
    work["_servico_norm"] = work[col_servico].astype(str).str.strip().str.upper()

    # --- Detecção por janela deslizante ---
    # Agora vamos guardar:
    # - OS original (legítima) E as duplicatas
    # - E marcar explicitamente se cada linha é ORIGINAL ou DUPLICATA
    grupo_counter     = 0
    registros_grupo   = []  # lista de dicts com info de cada linha (original + duplicatas)
    seen_indices      = set()  # evita colocar a mesma linha em mais de um grupo

    grupos = work.groupby(["_cliente_norm", "_servico_norm"])

    for (cliente, servico), grp in grupos:
        if len(grp) < 2:
            continue

        datas   = grp["_data_parsed"].tolist()
        indices = grp.index.tolist()

        i = 0
        while i < len(datas):
            cluster = [i]
            j = i + 1
            while j < len(datas):
                delta = (datas[j] - datas[i]).days
                if delta <= janela_dias:
                    cluster.append(j)
                    j += 1
                else:
                    break

            # Cluster representa OS muito próximas entre si
            if len(cluster) > 1:
                grupo_counter += 1

                # Primeiro do cluster = ORIGINAL
                original_idx_local = cluster[0]
                original_real_idx  = indices[original_idx_local]

                if original_real_idx not in seen_indices:
                    seen_indices.add(original_real_idx)
                    registros_grupo.append({
                        "grupo_duplicidade": grupo_counter,
                        "tipo_registro": "ORIGINAL",
                        "idx_real": original_real_idx,
                    })

                # Demais = DUPLICATAS
                for idx_local in cluster[1:]:
                    real_idx = indices[idx_local]
                    if real_idx not in seen_indices:
                        seen_indices.add(real_idx)
                        registros_grupo.append({
                            "grupo_duplicidade": grupo_counter,
                            "tipo_registro": "DUPLICATA",
                            "idx_real": real_idx,
                        })

                i = j
            else:
                i += 1

    if not registros_grupo:
        return pd.DataFrame(), pd.DataFrame()

    # Monta DataFrame com as linhas (original + duplicatas)
    linhas = []
    for reg in registros_grupo:
        linha = work.loc[reg["idx_real"]].copy()
        linha["grupo_duplicidade"] = reg["grupo_duplicidade"]
        linha["tipo_registro"]     = reg["tipo_registro"]
        linhas.append(linha)

    df_grouped = pd.DataFrame(linhas).copy()
    df_grouped = df_grouped.sort_values(["grupo_duplicidade", "_data_parsed", "tipo_registro"])

    # Colunas de saída detalhada
    output_cols = ["grupo_duplicidade", "tipo_registro"]
    if col_os:
        output_cols.append(col_os)
    output_cols += [col_cliente, col_servico, col_data]
    if cols_extras:
        output_cols += [c for c in cols_extras if c not in output_cols]

    df_dup_out = df_grouped[[c for c in output_cols if c in df_grouped.columns]].copy()
    df_dup_out[col_data] = df_grouped["_data_parsed"].dt.strftime("%d/%m/%Y")

    # --- Resumo por grupo ---
    resumo_rows = []
    for gid, grp in df_grouped.groupby("grupo_duplicidade"):
        grp_orig = grp[grp["tipo_registro"] == "ORIGINAL"]
        grp_dup  = grp[grp["tipo_registro"] == "DUPLICATA"]

        # Deve haver pelo menos 1 original
        if grp_orig.empty:
            continue

        original = grp_orig.iloc[0]
        datas_dup = grp_dup["_data_parsed"] if not grp_dup.empty else grp_orig["_data_parsed"]

        row = {
            "grupo":                    int(gid),
            "cliente":                  original[col_cliente],
            "tipo_servico":             original[col_servico],
            "data_os_original":         original["_data_parsed"].strftime("%d/%m/%Y"),
            "qtd_duplicatas":           len(grp_dup),
            "data_primeira_duplicata":  datas_dup.min().strftime("%d/%m/%Y") if not grp_dup.empty else "—",
            "data_ultima_duplicata":    datas_dup.max().strftime("%d/%m/%Y") if not grp_dup.empty else "—",
            "intervalo_dias_duplicatas": (datas_dup.max() - datas_dup.min()).days if not grp_dup.empty else 0,
        }

        if col_os:
            row["os_original"]   = str(original[col_os])
            row["os_duplicadas"] = ", ".join(grp_dup[col_os].astype(str).tolist()) if not grp_dup.empty else "—"

        resumo_rows.append(row)

    df_resumo = pd.DataFrame(resumo_rows)
    return df_dup_out, df_resumo


def to_excel_bytes(df_dup: pd.DataFrame, df_resumo: pd.DataFrame, janela_dias: int) -> bytes:
    wb = Workbook()

    header_fill  = PatternFill("solid", fgColor="1F4E79")
    header_font  = Font(bold=True, color="FFFFFF", size=11)
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin         = Side(style="thin", color="BFBFBF")
    border       = Border(left=thin, right=thin, top=thin, bottom=thin)

    # cores:
    palette_grupos = ["FFF2CC", "FDEBD0", "D5F5E3", "D6EAF8", "F9EBEA",
                      "EAF2FF", "FDF2F8", "E8F8F5", "FDFEFE", "F4ECF7"]
    fill_original  = PatternFill("solid", fgColor="C6EFCE")  # verde claro
    fill_dup_extra = PatternFill("solid", fgColor="FFC7CE")  # opcional para duplicatas sem grupo (se houver)

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

        # Descobre índice da coluna tipo_registro, se existir
        has_tipo = "tipo_registro" in df.columns

        for r_idx, row in enumerate(df.itertuples(index=False), 2):
            # Valor do grupo (para cor de fundo das duplicatas)
            grupo_val = None
            if grupo_col and grupo_col in df.columns:
                grupo_val = getattr(row, grupo_col.replace(" ", "_"), None)

            # tipo_registro: ORIGINAL / DUPLICATA
            tipo_val = None
            if has_tipo:
                tipo_val = getattr(row, "tipo_registro", None)

            # Define fill da linha
            row_fill = None
            if tipo_val == "ORIGINAL":
                row_fill = fill_original
            elif grupo_val is not None:
                color    = palette_grupos[(int(grupo_val) - 1) % len(palette_grupos)]
                row_fill = PatternFill("solid", fgColor=color)
            elif tipo_val == "DUPLICATA":
                row_fill = fill_dup_extra

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
        ("Gerado em",                  datetime.now().strftime("%d/%m/%Y %H:%M")),
        ("Janela de análise (dias)",   janela_dias),
        ("Total de grupos",            df_resumo["grupo"].nunique() if not df_resumo.empty else 0),
        ("Total de OS duplicadas",     int(df_dup[df_dup["tipo_registro"] == "DUPLICATA"].shape[0])
                                       if not df_dup.empty and "tipo_registro" in df_dup.columns else 0),
    ]
    for r, (k, v) in enumerate(meta, 1):
        ws3.cell(r, 1, k).font = Font(bold=True)
        ws3.cell(r, 2, str(v))

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()

# ── Interface ────────────────────────────────────────────────────────────────
st.title("🔍 Detector de Duplicidades de Ordens de Serviço")
st.caption("Identifica OS abertas para o mesmo cliente com o mesmo tipo de serviço dentro de um período configurável.")

st.divider()

uploaded = st.file_uploader(
    "📂 Selecione o arquivo de OS (.xlsx, .xls ou .csv)",
    type=["xlsx", "xls", "csv"],
)

if uploaded:
    df = load_file(uploaded)

    if df is not None:
        st.success(f"Arquivo carregado: **{uploaded.name}** — {len(df)} linhas × {len(df.columns)} colunas")

        with st.expander("👁️ Prévia do arquivo (primeiras 5 linhas)", expanded=False):
            st.dataframe(df.head(5), use_container_width=True)

        st.divider()
        st.subheader("⚙️ Configurações da análise")

        all_cols = list(df.columns)
        none_opt = "— nenhuma —"

        c1, c2, c3 = st.columns(3)

        def suggest(keywords):
            for kw in keywords:
                for col in all_cols:
                    if kw.lower() in col.lower():
                        return col
            return all_cols[0]

        col_cliente = c1.selectbox(
            "👤 Coluna de identificação do cliente",
            all_cols,
            index=all_cols.index(suggest(["cliente", "matricula", "cpf", "cod", "id_cli"])),
        )
        col_servico = c2.selectbox(
            "🔧 Coluna de tipo de serviço",
            all_cols,
            index=all_cols.index(suggest(["servico", "serviço", "tipo", "categoria", "os_tipo"])),
        )
        col_data = c3.selectbox(
            "📅 Coluna de data da OS",
            all_cols,
            index=all_cols.index(suggest(["data", "dt_", "abertura", "criacao", "criação"])),
        )

        c4, c5 = st.columns(2)

        col_os = c4.selectbox(
            "🔢 Coluna de número da OS (opcional)",
            [none_opt] + all_cols,
            index=0,
        )
        col_os = None if col_os == none_opt else col_os

        janela_dias = c5.number_input(
            "📆 Janela de tempo para considerar duplicidade (dias)",
            min_value=1, max_value=3650, value=30, step=1,
        )

        extras_disponiveis = [c for c in all_cols if c not in [col_cliente, col_servico, col_data, col_os]]
        cols_extras = st.multiselect(
            "➕ Colunas adicionais para exibir no relatório (opcional)",
            extras_disponiveis,
        )

        st.divider()

        with st.expander("🗓️ Filtrar período de análise (opcional)", expanded=False):
            use_periodo = st.checkbox("Ativar filtro de período")
            if use_periodo:
                f1, f2 = st.columns(2)
                data_ini = f1.date_input("Data inicial", value=datetime.today() - timedelta(days=365))
                data_fim = f2.date_input("Data final",   value=datetime.today())
            else:
                data_ini = data_fim = None

        if st.button("🚀 Analisar duplicidades", type="primary", use_container_width=True):
            with st.spinner("Analisando..."):

                df_work = df.copy()

                if use_periodo and data_ini and data_fim:
                    df_work["_data_temp"] = parse_dates_robust(df_work[col_data].astype(str).str.strip())
                    df_work = df_work[
                        (df_work["_data_temp"] >= pd.Timestamp(data_ini)) &
                        (df_work["_data_temp"] <= pd.Timestamp(data_fim))
                    ].drop(columns=["_data_temp"])
                    st.info(f"Período filtrado: {data_ini.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')} — {len(df_work)} registros.")

                df_dup, df_resumo = detect_duplicates(
                    df=df_work,
                    col_cliente=col_cliente,
                    col_servico=col_servico,
                    col_data=col_data,
                    janela_dias=int(janela_dias),
                    col_os=col_os,
                    cols_extras=cols_extras if cols_extras else None,
                )

                st.session_state["df_dup"]    = df_dup
                st.session_state["df_resumo"] = df_resumo
                st.session_state["janela"]    = int(janela_dias)

        if "df_dup" in st.session_state:
            df_dup    = st.session_state["df_dup"]
            df_resumo = st.session_state["df_resumo"]
            janela    = st.session_state["janela"]

            st.divider()

            if df_dup.empty:
                st.success("✅ Nenhuma duplicidade encontrada com os parâmetros informados.")
            else:
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Grupos duplicados",    df_resumo["grupo"].nunique())
                k2.metric("Total de OS afetadas", len(df_dup))
                k3.metric("Janela utilizada",     f"{janela} dias")
                k4.metric("Serviços únicos dup.", df_resumo["tipo_servico"].nunique())

                st.divider()

                tab1, tab2 = st.tabs(["📄 Detalhamento por OS", "📊 Resumo por Grupo"])

                with tab1:
                    st.caption("Cada grupo mostra a OS ORIGINAL (em verde) e as OS DUPLICATAS abertas depois dela.")
                    st.dataframe(df_dup, use_container_width=True, height=450)

                    # métricas
                k2.metric(
                    "Total de OS duplicadas",
                    int(df_dup[df_dup["tipo_registro"] == "DUPLICATA"].shape[0]) if not df_dup.empty else 0
                )

                with tab2:
                    st.dataframe(df_resumo, use_container_width=True, height=450)

                st.divider()

                xlsx_bytes = to_excel_bytes(df_dup, df_resumo, janela)
                st.download_button(
                    label="⬇️ Baixar relatório XLSX",
                    data=xlsx_bytes,
                    file_name=f"duplicidades_os_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )