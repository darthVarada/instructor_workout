import pandas as pd
import s3fs
import os
from datetime import datetime

# ============================
# CONFIGURAÇÕES
# ============================

BUCKET_BASE = "s3://instructor-workout-datas"

PASTAS = [
    f"{BUCKET_BASE}/bronze/",
    f"{BUCKET_BASE}/silver/",
    f"{BUCKET_BASE}/gold/",
]

# Caminho para salvar o arquivo TXT no Downloads do usuário
DOWNLOADS = os.path.join(os.path.expanduser("~"), "Downloads")
output_file = os.path.join(
    DOWNLOADS, f"s3_inspecao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
)

# Garantir que o diretório existe
os.makedirs(DOWNLOADS, exist_ok=True)


# ============================
# FUNÇÕES
# ============================

def ler_primeiras_linhas(fs: s3fs.S3FileSystem, path: str, n: int = 5):
    """Lê primeiras linhas de parquet ou csv e retorna como texto."""
    try:
        if path.endswith(".parquet"):
            df = pd.read_parquet(path, filesystem=fs)
        elif path.endswith(".csv"):
            df = pd.read_csv(
                path,
                nrows=n,
                storage_options={"anon": False}
            )
        else:
            return "(arquivo não suportado)"

        return df.head(n).to_string(index=False)

    except Exception as e:
        return f"[ERRO ao ler arquivo]: {e}"


def escrever(linhas: str):
    """Escreve texto no arquivo de saída."""
    with open(output_file, "a", encoding="utf-8") as f:
        f.write(linhas + "\n")


def main():
    fs = s3fs.S3FileSystem(anon=False)

    escrever("=== INSPEÇÃO COMPLETA DAS TABELAS NO S3 ===\n")

    for base in PASTAS:
        escrever("\n" + "#" * 80)
        escrever(f"PASTA BASE: {base}")
        escrever("#" * 80)

        for dirpath, dirs, files in fs.walk(base):
            if not files:
                continue

            escrever(f"\n📁 PASTA: {dirpath}")

            for filename in sorted(files):
                full_path = dirpath.rstrip("/") + "/" + filename

                escrever(f"\n--- ARQUIVO: {full_path} ---")
                conteudo = ler_primeiras_linhas(fs, full_path)
                escrever(conteudo)

    escrever("\n\n✔ FINALIZADO COM SUCESSO!")

    print(f"\nArquivo gerado em:\n👉 {output_file}\n")


# ============================
# EXECUÇÃO
# ============================
if __name__ == "__main__":
    main()
