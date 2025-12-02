import pandas as pd
from pathlib import Path

# =========================
# CAMINHO DOS ARQUIVOS (DOWNLOADS)
# =========================

BASE_PATH = Path.home() / "Downloads"

FILES = {
    "exercises_dim": BASE_PATH / "exercises_dim.parquet",
    "muscles_bridge": BASE_PATH / "muscles_bridge.parquet",
    "instructions_bridge": BASE_PATH / "instructions_bridge.parquet",
}

# =========================
# FUNÇÃO DE LEITURA E INSPEÇÃO
# =========================

def analisar_parquet(nome, caminho):
    print("=" * 80)
    print(f"📊 TABELA: {nome}")
    print(f"📁 ARQUIVO: {caminho}")
    
    if not caminho.exists():
        print("❌ ARQUIVO NÃO ENCONTRADO!")
        return
    
    df = pd.read_parquet(caminho)

    print(f"\n✅ TOTAL DE REGISTROS: {len(df)}")
    print("\n✅ COLUNAS E TIPOS:")
    print(df.dtypes)

    print("\n✅ AMOSTRA DOS DADOS:")
    print(df.head(5))

    print("\n✅ VALORES NULOS POR COLUNA:")
    print(df.isnull().sum())

# =========================
# EXECUÇÃO
# =========================

if __name__ == "__main__":
    print("\n🚀 INICIANDO ANÁLISE DOS PARQUETS DA GOLD (LOCAL)\n")

    for nome, caminho in FILES.items():
        analisar_parquet(nome, caminho)

    print("\n✅ ANÁLISE FINALIZADA\n")
