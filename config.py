# config.py
from pathlib import Path

# ── Diretórios ───────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).parent
DATA_DIR        = BASE_DIR / "data"
INCREMENTOS_DIR = DATA_DIR / "incrementos"

DATA_DIR.mkdir(exist_ok=True)
INCREMENTOS_DIR.mkdir(exist_ok=True)

# ── Arquivo mestre ───────────────────────────────────────────────────────────
PARQUET_MESTRE  = DATA_DIR / "os_historico.parquet"

# ── Formatos de data suportados ──────────────────────────────────────────────
DATE_FORMATS = [
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

# ── Colunas obrigatórias no Parquet mestre ───────────────────────────────────
# Serão criadas durante a ingestão se não existirem no arquivo de origem
COLUNAS_SISTEMA = ["_data_parsed", "_cliente_norm", "_servico_norm", "_lote"]
