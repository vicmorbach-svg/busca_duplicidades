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
    return duckdb.connect()


@st.cache_data(show_spinner="⚡ Lendo base histórica via DuckDB...")
def carregar_base(
    data_ini: str | None = None,
    data_fim: str | None = None,
    lotes: list[str] | None = None,
) -> pd.DataFrame:
    if not PARQUET_MESTRE.exists():
        return pd.DataFrame()

    con   = get_con()
    where = []

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
    query = f"SELECT DISTINCT _lote FROM read_parquet('{PARQUET_MESTRE}') ORDER BY _lote"
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
            COUNT(*)                       AS total_registros,
            COUNT(DISTINCT _lote)          AS total_lotes,
            MIN(_data_parsed)::VARCHAR     AS data_min,
            MAX(_data_parsed)::VARCHAR     AS data_max,
            COUNT(DISTINCT _cliente_norm)  AS clientes_unicos,
            COUNT(DISTINCT _servico_norm)  AS servicos_unicos
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
        result[still_null] = pd.to_datetime(
            series[still_null], infer_datetime_format=True, errors="coerce"
        )
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
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Retorna (df_detalhamento, df_resumo_grupo, df_resumo_servico).

    Lógica correta:
    - Agrupa por (cliente, tipo_serviço)
    - Ordena por data
    - Para cada OS, verifica quantas outras OS do mesmo grupo
      estão dentro da janela de dias APÓS ela
    - A primeira OS de cada grupo de duplicidade é marcada ORIGINAL
    - As demais são DUPLICATAS e entram na contagem
    - Uma OS pode ser ORIGINAL de um grupo e não aparecer
      como duplicata de outro (cada OS só recebe um papel)
    """

    work = df.copy()

    # ── Parsing de datas ──────────────────────────────────────────────────────
    if "_data_parsed" in work.columns:
        work["_data_parsed"] = pd.to_datetime(work["_data_parsed"], errors="coerce")
    else:
        work["_data_parsed"] = parse_dates_robust(work[col_data].astype(str).str.strip())

    total     = len(work)
    invalidas = work["_data_parsed"].isna().sum()
    validas   = total - invalidas

    with st.expander("📅 Diagnóstico de datas", expanded=invalidas > 0):
        d1, d2, d3 = st.columns(3)
        d1.metric("Total de registros",        f"{total:,}")
        d2.metric("Datas reconhecidas",         f"{validas:,}")
        d3.metric("Datas inválidas/ignoradas",  f"{invalidas:,}",
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

    # ── Prepara working set ───────────────────────────────────────────────────
    work = work.dropna(subset=["_data_parsed"]).copy()
    work["_cliente_norm"] = work[col_cliente].astype(str).str.strip().str.upper()
    work["_servico_norm"] = work[col_servico].astype(str).str.strip().str.upper()

    # índice sequencial único para cada linha
    work = work.reset_index(drop=True)
    work["_row_id"] = work.index

    janela = pd.Timedelta(days=janela_dias)

    # ── Algoritmo de detecção ─────────────────────────────────────────────────
    #
    # Para cada par (cliente, serviço):
    #   - Ordena as OS por data
    #   - Percorre cada OS como possível "âncora" (ORIGINAL)
    #   - Todas as OS dentro da janela APÓS a âncora são DUPLICATAS daquele grupo
    #   - Uma OS já classificada como DUPLICATA não pode ser âncora
    #     de um novo grupo (evita dupla contagem)
    #
    registros = []   # lista final com todas as linhas classificadas

    for (cliente, servico), grp in work.groupby(["_cliente_norm", "_servico_norm"], sort=False):
        if len(grp) < 2:
            continue

        grp     = grp.sort_values("_data_parsed").reset_index(drop=True)
        datas   = grp["_data_parsed"].tolist()
        row_ids = grp["_row_id"].tolist()
        n       = len(grp)

        classificacao = {}   # row_id -> {"grupo": int, "tipo": str}
        grupo_counter = 0
        i = 0

        while i < n:
            rid_i = row_ids[i]

            # Se já foi classificado como DUPLICATA, não pode ser âncora
            if rid_i in classificacao and classificacao[rid_i]["tipo"] == "DUPLICATA":
                i += 1
                continue

            # Busca todas as OS dentro da janela a partir de datas[i]
            duplicatas_encontradas = []
            for j in range(i + 1, n):
                delta = (datas[j] - datas[i]).days
                if delta <= janela_dias:
                    rid_j = row_ids[j]
                    # Só entra se ainda não foi classificada
                    if rid_j not in classificacao:
                        duplicatas_encontradas.append(j)
                else:
                    break   # já está ordenado, pode parar

            if duplicatas_encontradas:
                grupo_counter += 1

                # Marca a âncora como ORIGINAL (se ainda não foi marcada)
                if rid_i not in classificacao:
                    classificacao[rid_i] = {"grupo": grupo_counter, "tipo": "ORIGINAL"}

                # Marca as duplicatas
                for j in duplicatas_encontradas:
                    rid_j = row_ids[j]
                    classificacao[rid_j] = {"grupo": grupo_counter, "tipo": "DUPLICATA"}

            i += 1

        # Adiciona à lista de resultado
        for idx in range(n):
            rid = row_ids[idx]
            if rid in classificacao:
                registros.append({
                    "_row_id":           rid,
                    "grupo_duplicidade":  classificacao[rid]["grupo"],
                    "tipo_registro":     classificacao[rid]["tipo"],
                })

    if not registros:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # ── Monta DataFrame de saída ──────────────────────────────────────────────
    df_class  = pd.DataFrame(registros).drop_duplicates("_row_id")
    df_merged = work.merge(df_class, on="_row_id", how="inner")
    df_merged = df_merged.sort_values(["grupo_duplicidade", "_data_parsed"]).reset_index(drop=True)

    # Colunas de saída do detalhamento
    output_cols = ["grupo_duplicidade", "tipo_registro"]
    if col_os:
        output_cols.append(col_os)
    output_cols += [col_cliente, col_servico, col_data]
    if cols_extras:
        output_cols += [c for c in cols_extras if c not in output_cols]

    df_det = df_merged[[c for c in output_cols if c in df_merged.columns]].copy()
    df_det[col_data] = df_merged["_data_parsed"].dt.strftime("%d/%m/%Y")

    # ── Resumo por grupo ──────────────────────────────────────────────────────
    resumo_grupos = []
    for gid, grp in df_merged.groupby("grupo_duplicidade"):
        orig = grp[grp["tipo_registro"] == "ORIGINAL"]
        dups = grp[grp["tipo_registro"] == "DUPLICATA"]

        if orig.empty:
            continue

        o         = orig.iloc[0]
        datas_dup = dups["_data_parsed"]

        row = {
            "grupo":                     int(gid),
            "cliente":                   o[col_cliente],
            "tipo_servico":              o[col_servico],
            "data_os_original":          o["_data_parsed"].strftime("%d/%m/%Y"),
            "qtd_duplicatas":            len(dups),
            "data_primeira_duplicata":   datas_dup.min().strftime("%d/%m/%Y") if not dups.empty else "—",
            "data_ultima_duplicata":     datas_dup.max().strftime("%d/%m/%Y") if not dups.empty else "—",
            "intervalo_dias":            (datas_dup.max() - datas_dup.min()).days if not dups.empty else 0,
        }
        if col_os:
            row["os_original"]   = str(o[col_os])
            row["os_duplicadas"] = ", ".join(dups[col_os].astype(str).tolist())

        resumo_grupos.append(row)

    df_resumo_grupos = pd.DataFrame(resumo_grupos)

    # ── Resumo por tipo de serviço ────────────────────────────────────────────
    #
    # Para cada tipo de serviço:
    #   - Total de OS duplicadas (sem contar originais)
    #   - Clientes únicos afetados
    #   - Distribuição: faixas de quantidades de pedidos por cliente
    #
    resumo_servico = []

    # Base para distribuição: todas as OS (originais + duplicatas) por cliente+serviço
    # para calcular quantos pedidos cada cliente fez no período
    contagem_cliente_servico = (
        work.groupby(["_servico_norm", "_cliente_norm"])
        .size()
        .reset_index(name="total_os_cliente")
    )

    # Só interessa serviços que tiveram duplicatas
    servicos_com_dup = df_merged[df_merged["tipo_registro"] == "DUPLICATA"][col_servico].unique()

    for servico in servicos_com_dup:
        servico_norm = str(servico).strip().upper()

        # duplicatas deste serviço
        dups_serv = df_merged[
            (df_merged["tipo_registro"] == "DUPLICATA") &
            (df_merged["_servico_norm"] == servico_norm)
        ]

        # clientes únicos que geraram duplicata neste serviço
        clientes_dup = dups_serv["_cliente_norm"].nunique()

        # total de OS do serviço no período (base completa, não só duplicatas)
        total_os_servico = work[work["_servico_norm"] == servico_norm].shape[0]

        # distribuição de pedidos por cliente (base completa do período)
        dist = contagem_cliente_servico[
            contagem_cliente_servico["_servico_norm"] == servico_norm
        ]["total_os_cliente"]

        resumo_servico.append({
            "tipo_servico":              servico,
            "total_os_no_periodo":       total_os_servico,
            "total_duplicatas":          len(dups_serv),
            "clientes_com_duplicata":    clientes_dup,
            "media_duplicatas_cliente":  round(len(dups_serv) / clientes_dup, 2) if clientes_dup else 0,
            # distribuição de faixas
            "clientes_1_pedido":         int((dist == 1).sum()),
            "clientes_2_pedidos":        int((dist == 2).sum()),
            "clientes_3_pedidos":        int((dist == 3).sum()),
            "clientes_4_a_6_pedidos":    int(((dist >= 4) & (dist <= 6)).sum()),
            "clientes_7_a_10_pedidos":   int(((dist >= 7) & (dist <= 10)).sum()),
            "clientes_mais_10_pedidos":  int((dist > 10).sum()),
        })

    df_resumo_servico = pd.DataFrame(resumo_servico).sort_values(
        "total_duplicatas", ascending=False
    ).reset_index(drop=True)

    return df_det, df_resumo_grupos, df_resumo_servico


# ── Exportação Excel ──────────────────────────────────────────────────────────
def to_excel_bytes(
    df_det: pd.DataFrame,
    df_grupos: pd.DataFrame,
    df_servico: pd.DataFrame,
    janela_dias: int,
) -> bytes:
    wb = Workbook()

    header_fill   = PatternFill("solid", fgColor="1F4E79")
    header_font   = Font(bold=True, color="FFFFFF", size=11)
    header_align  = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin          = Side(style="thin", color="BFBFBF")
    border        = Border(left=thin, right=thin, top=thin, bottom=thin)
    fill_original = PatternFill("solid", fgColor="C6EFCE")
    palette       = ["FFF2CC", "FDEBD0", "D5F5E3", "D6EAF8", "F9EBEA",
                     "EAF2FF", "FDF2F8", "E8F8F5", "FDFEFE", "F4ECF7"]

    def auto_col_width(ws, df):
        for c_

