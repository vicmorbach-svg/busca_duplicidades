# atualiza_base.py
"""
Script de ingestão mensal.
Uso:
    python atualiza_base.py --arquivo caminho/para/arquivo.xlsx \
                            --col_cliente matricula             \
                            --col_servico tipo_servico          \
                            --col_data    data_abertura

Opções:
    --col_os      coluna do número da OS (opcional)
    --lote        nome/identificador do lote (opcional, padrão = YYYY-MM)
    --force       sobrescreve lote existente em vez de rejeitar
"""

import argparse
import hashlib
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import (
    DATE_FORMATS,
    INCREMENTOS_DIR,
    PARQUET_MESTRE,
)


# ── Helpers ──────────────────────────────────────────────────────────────────
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


def ler_arquivo(caminho: str) -> pd.DataFrame:
    caminho = caminho.strip()
    if caminho.endswith(".csv"):
        df = pd.read_csv(caminho, dtype=str)
    elif caminho.endswith((".xlsx", ".xls")):
        df = pd.read_excel(caminho, dtype=str)
    else:
        raise ValueError(f"Formato não suportado: {caminho}")

    df.columns = df.columns.str.strip()
    return df


def hash_arquivo(caminho: str) -> str:
    h = hashlib.md5()
    with open(caminho, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def lote_ja_existe(lote: str) -> bool:
    if not PARQUET_MESTRE.exists():
        return False
    table  = pq.read_table(PARQUET_MESTRE, columns=["_lote"])
    lotes  = table.column("_lote").to_pylist()
    return lote in set(lotes)


def salvar_incremento(df: pd.DataFrame, lote: str):
    path = INCREMENTOS_DIR / f"{lote}.parquet"
    df.to_parquet(path, index=False, compression="snappy")
    print(f"  → Incremento salvo em: {path}")


def atualizar_mestre(df_novo: pd.DataFrame):
    if PARQUET_MESTRE.exists():
        df_antigo = pd.read_parquet(PARQUET_MESTRE)
        df_total  = pd.concat([df_antigo, df_novo], ignore_index=True)
    else:
        df_total = df_novo

    # Garante tipos corretos antes de salvar
    if "_data_parsed" in df_total.columns:
        df_total["_data_parsed"] = pd.to_datetime(df_total["_data_parsed"], errors="coerce")

    df_total.to_parquet(PARQUET_MESTRE, index=False, compression="snappy")
    print(f"  → Base mestre atualizada: {len(df_total):,} registros totais.")
    return df_total


def processar(
    caminho: str,
    col_cliente: str,
    col_servico: str,
    col_data: str,
    col_os: str | None,
    lote: str,
    force: bool,
):
    print(f"\n{'='*60}")
    print(f"  Lote:    {lote}")
    print(f"  Arquivo: {caminho}")
    print(f"{'='*60}")

    # ── Verifica duplicidade de lote ─────────────────────────────────────────
    if lote_ja_existe(lote) and not force:
        print(f"\n⚠️  O lote '{lote}' já existe na base mestre.")
        print("   Use --force para sobrescrever ou escolha outro nome de lote.")
        sys.exit(1)

    # ── Lê arquivo ───────────────────────────────────────────────────────────
    print("\n📂 Lendo arquivo...")
    df = ler_arquivo(caminho)
    print(f"   {len(df):,} linhas × {len(df.columns)} colunas lidas.")

    # ── Valida colunas obrigatórias ──────────────────────────────────────────
    obrigatorias = [col_cliente, col_servico, col_data]
    if col_os:
        obrigatorias.append(col_os)

    faltando = [c for c in obrigatorias if c not in df.columns]
    if faltando:
        print(f"\n❌ Colunas não encontradas no arquivo: {faltando}")
        print(f"   Colunas disponíveis: {list(df.columns)}")
        sys.exit(1)

    # ── Parsing de datas ─────────────────────────────────────────────────────
    print("\n📅 Processando datas...")
    df["_data_parsed"] = parse_dates_robust(df[col_data].astype(str).str.strip())

    invalidas = df["_data_parsed"].isna().sum()
    validas   = len(df) - invalidas
    print(f"   ✅ {validas:,} datas reconhecidas | ⚠️  {invalidas:,} inválidas/ignoradas")

    if invalidas > 0:
        exemplos = df[df["_data_parsed"].isna()][col_data].dropna().unique()[:5]
        print(f"   Exemplos inválidos: {list(exemplos)}")

    # ── Normalização ─────────────────────────────────────────────────────────
    print("\n🔧 Normalizando colunas de chave...")
    df["_cliente_norm"] = df[col_cliente].astype(str).str.strip().str.upper()
    df["_servico_norm"] = df[col_servico].astype(str).str.strip().str.upper()
    df["_lote"]         = lote

    # ── Metadados de controle ────────────────────────────────────────────────
    df["_ingestao_ts"] = datetime.now().isoformat()
    df["_hash_arquivo"] = hash_arquivo(caminho)

    # ── Salva incremento individual ──────────────────────────────────────────
    print("\n💾 Salvando...")
    salvar_incremento(df, lote)

    # ── Atualiza mestre ──────────────────────────────────────────────────────
    df_total = atualizar_mestre(df)

    print(f"\n✅ Ingestão do lote '{lote}' concluída com sucesso!")
    print(f"   Registros no lote:  {len(df):,}")
    print(f"   Registros no mestre: {len(df_total):,}")
    print(f"{'='*60}\n")


# ── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestão mensal de OS para base Parquet.")
    parser.add_argument("--arquivo",      required=True,  help="Caminho do arquivo .xlsx/.csv")
    parser.add_argument("--col_cliente",  required=True,  help="Nome da coluna de cliente")
    parser.add_argument("--col_servico",  required=True,  help="Nome da coluna de tipo de serviço")
    parser.add_argument("--col_data",     required=True,  help="Nome da coluna de data")
    parser.add_argument("--col_os",       default=None,   help="Nome da coluna de número da OS (opcional)")
    parser.add_argument("--lote",         default=datetime.now().strftime("%Y-%m"),
                                          help="Identificador do lote (padrão: YYYY-MM)")
    parser.add_argument("--force",        action="store_true",
                                          help="Sobrescreve lote se já existir")

    args = parser.parse_args()

    processar(
        caminho=args.arquivo,
        col_cliente=args.col_cliente,
        col_servico=args.col_servico,
        col_data=args.col_data,
        col_os=args.col_os,
        lote=args.lote,
        force=args.force,
    )
