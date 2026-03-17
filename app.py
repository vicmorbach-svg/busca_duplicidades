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
    """
    Retorna (df_duplicatas, df_resumo).
    Duplicata = mesmo cliente + mesmo tipo de serviço dentro de `janela_dias`.
    """

    work = df.copy()

    # Converte data
    work["_data_parsed"] = pd.to_datetime(work[col_data], dayfirst=True, errors="coerce")
    invalidas = work["_data_parsed"].isna().sum()
    if invalidas > 0:
        st.warning(f"⚠️ {invalidas} linha(s) com data inválida foram ignoradas na análise.")

    work = work.dropna(subset=["_data_parsed"]).copy()
    work = work.sort_values(["_data_parsed"]).reset_index(drop=True)

    # Normaliza cliente e serviço para comparação case-insensitive
    work["_cliente_norm"] = work[col_cliente].astype(str).str.strip().str.upper()
    work["_servico_norm"] = work[col_servico].astype(str).str.strip().str.upper()

    # Identifica grupos duplicados por janela deslizante
    dup_indices = set()
    grupo_id_map = {}  # index -> grupo_id
    grupo_counter = 0

    # Agrupa por (cliente, serviço) e verifica proximidade temporal
    grupos = work.groupby(["_cliente_norm", "_servico_norm"])

    for (cliente, servico), grp in grupos:
        if len(grp) < 2:
            continue

        datas = grp["_data_parsed"].tolist()
        indices = grp.index.tolist()

        # Janela deslizante: compara cada OS com as próximas
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

            if len(cluster) > 1:
                grupo_counter += 1
                for idx in cluster:
                    real_idx = indices[idx]
                    dup_indices.add(real_idx)
                    grupo_id_map[real_idx] = grupo_counter
                i = j  # avança para após o cluster
            else:
                i += 1

    if not dup_indices:
        return pd.DataFrame(), pd.DataFrame()

    df_dup = work.loc[sorted(dup_indices)].copy()
    df_dup["grupo_duplicidade"] = df_dup.index.map(grupo_id_map)
    df_dup = df_dup.sort_values(["grupo_duplicidade", "_data_parsed"])

    # Monta colunas de saída
    output_cols = ["grupo_duplicidade"]
    if col_os:
        output_cols.append(col_os)
    output_cols += [col_cliente, col_servico, col_data]
    if cols_extras:
        output_cols += [c for c in cols_extras if c not in output_cols]

    df_dup_out = df_dup[[c for c in output_cols if c in df_dup.columns]].copy()
    df_dup_out[col_data] = df_dup["_data_parsed"].dt.strftime("%d/%m/%Y")

    # Resumo por grupo
    resumo_rows = []
    for gid, grp in df_dup.groupby("grupo_duplicidade"):
        datas_grp = grp["_data_parsed"]
        resumo_rows.append({
            "grupo": int(gid),
            "cliente": grp[col_cliente].iloc[0],
            "tipo_servico": grp[col_servico].iloc[0],
            "qtd_os": len(grp),
            "data_mais_antiga": datas_grp.min().strftime("%d/%m/%Y"),
            "data_mais_recente": datas_grp.max().strftime("%d/%m/%Y"),
            "intervalo_dias": (datas_grp.max() - datas_grp.min()).days,
            **({"os_envolvidas": ", ".join(grp[col_os].astype(str).tolist())} if col_os else {}),
        })

    df_resumo = pd.DataFrame(resumo_rows)
    return df_dup_out, df_resumo


def to_excel_bytes(df_dup: pd.DataFrame, df_resumo: pd.DataFrame, janela_dias: int) -> bytes:
    wb = Workbook()

    # ── Paleta de cores ─────────────────────────────────────────────────────
    header_fill  = PatternFill("solid", fgColor="1F4E79")
    header_font  = Font(bold=True, color="FFFFFF", size=11)
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin         = Side(style="thin", color="BFBFBF")
    border       = Border(left=thin, right=thin, top=thin, bottom=thin)

    # Cores alternadas por grupo
    palette = ["FFF2CC", "FDEBD0", "D5F5E3", "D6EAF8", "F9EBEA",
               "EAF2FF", "FDF2F8", "E8F8F5", "FDFEFE", "F4ECF7"]

    def write_sheet(ws, df, title, grupo_col=None):
        ws.title = title
        if df.empty:
            ws.cell(1, 1, "Nenhum dado para exibir.")
            return

        # Cabeçalho
        for c_idx, col in enumerate(df.columns, 1):
            cell = ws.cell(row=1, column=c_idx, value=col)
            cell.fill  = header_fill
            cell.font  = header_font
            cell.alignment = header_align
            cell.border = border
            ws.column_dimensions[get_column_letter(c_idx)].width = max(16, len(str(col)) + 6)
        ws.row_dimensions[1].height = 28

        # Dados
        for r_idx, row in enumerate(df.itertuples(index=False), 2):
            grupo_val = None
            if grupo_col and grupo_col in df.columns:
                grupo_val = getattr(row, grupo_col.replace(" ", "_"), None)

            row_fill = None
            if grupo_val is not None:
                color = palette[(int(grupo_val) - 1) % len(palette)]
                row_fill = PatternFill("solid", fgColor=color)

            for c_idx, val in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx, value=val if pd.notna(val) else None)
                cell.border = border
                cell.alignment = Alignment(vertical="center")
                if row_fill:
                    cell.fill = row_fill

        ws.freeze_panes = "A2"

    # Aba 1 — Detalhamento
    ws1 = wb.active
    write_sheet(ws1, df_dup, "Duplicidades", grupo_col="grupo_duplicidade")

    # Aba 2 — Resumo
    ws2 = wb.create_sheet("Resumo por Grupo")
    write_sheet(ws2, df_resumo, "Resumo por Grupo", grupo_col="grupo")

    # Aba 3 — Metadados
    ws3 = wb.create_sheet("Configurações")
    ws3.column_dimensions["A"].width = 30
    ws3.column_dimensions["B"].width = 30
    meta = [
        ("Gerado em", datetime.now().strftime("%d/%m/%Y %H:%M")),
        ("Janela de análise (dias)", janela_dias),
        ("Total de grupos duplicados", df_resumo["grupo"].nunique() if not df_resumo.empty else 0),
        ("Total de OS duplicadas", len(df_dup)),
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

# ── Upload ───────────────────────────────────────────────────────────────────
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

        # Tenta sugerir colunas por nome comum
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
            help="Coluna que identifica unicamente o cliente",
        )

        col_servico = c2.selectbox(
            "🔧 Coluna de tipo de serviço",
            all_cols,
            index=all_cols.index(suggest(["servico", "serviço", "tipo", "categoria", "os_tipo"])),
            help="Coluna que descreve o tipo/categoria da OS",
        )

        col_data = c3.selectbox(
            "📅 Coluna de data da OS",
            all_cols,
            index=all_cols.index(suggest(["data", "dt_", "abertura", "criacao", "criação"])),
            help="Coluna com a data de abertura (ou criação) da OS",
        )

        c4, c5 = st.columns(2)

        col_os = c4.selectbox(
            "🔢 Coluna de número da OS (opcional)",
            [none_opt] + all_cols,
            index=0,
            help="Se selecionada, aparecerá no relatório de duplicidades",
        )
        col_os = None if col_os == none_opt else col_os

        janela_dias = c5.number_input(
            "📆 Janela de tempo para considerar duplicidade (dias)",
            min_value=1,
            max_value=3650,
            value=30,
            step=1,
            help="OS do mesmo cliente + mesmo serviço abertas dentro deste intervalo serão marcadas como duplicadas",
        )

        # Colunas extras para o relatório
        extras_disponiveis = [c for c in all_cols if c not in [col_cliente, col_servico, col_data, col_os]]
        cols_extras = st.multiselect(
            "➕ Colunas adicionais para exibir no relatório (opcional)",
            extras_disponiveis,
        )

        st.divider()

        # Filtro de período global (opcional)
        with st.expander("🗓️ Filtrar período de análise (opcional)", expanded=False):
            use_periodo = st.checkbox("Ativar filtro de período")
            if use_periodo:
                f1, f2 = st.columns(2)
                data_ini = f1.date_input("Data inicial", value=datetime.today() - timedelta(days=365))
                data_fim = f2.date_input("Data final", value=datetime.today())
            else:
                data_ini = data_fim = None

        # ── Botão de análise ─────────────────────────────────────────────────
        if st.button("🚀 Analisar duplicidades", type="primary", use_container_width=True):
            with st.spinner("Analisando..."):

                df_work = df.copy()

                # Aplica filtro de período se ativado
                if use_periodo and data_ini and data_fim:
                    df_work["_data_temp"] = pd.to_datetime(df_work[col_data], dayfirst=True, errors="coerce")
                    df_work = df_work[
                        (df_work["_data_temp"] >= pd.Timestamp(data_ini)) &
                        (df_work["_data_temp"] <= pd.Timestamp(data_fim))
                    ].drop(columns=["_data_temp"])
                    st.info(f"Período filtrado: {data_ini.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')} — {len(df_work)} registros no período.")

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

        # ── Resultados ───────────────────────────────────────────────────────
        if "df_dup" in st.session_state:
            df_dup    = st.session_state["df_dup"]
            df_resumo = st.session_state["df_resumo"]
            janela    = st.session_state["janela"]

            st.divider()

            if df_dup.empty:
                st.success("✅ Nenhuma duplicidade encontrada com os parâmetros informados.")
            else:
                # KPIs
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Grupos duplicados",    df_resumo["grupo"].nunique())
                k2.metric("Total de OS afetadas", len(df_dup))
                k3.metric("Janela utilizada",     f"{janela} dias")
                k4.metric("Serviços únicos dup.", df_resumo["tipo_servico"].nunique())

                st.divider()

                tab1, tab2 = st.tabs(["📄 Detalhamento por OS", "📊 Resumo por Grupo"])

                with tab1:
                    st.dataframe(df_dup, use_container_width=True, height=450)

                with tab2:
                    st.dataframe(df_resumo, use_container_width=True, height=450)

                st.divider()

                # Download
                xlsx_bytes = to_excel_bytes(df_dup, df_resumo, janela)
                st.download_button(
                    label="⬇️ Baixar relatório XLSX",
                    data=xlsx_bytes,
                    file_name=f"duplicidades_os_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
