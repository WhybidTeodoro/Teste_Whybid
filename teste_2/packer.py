from __future__ import annotations

import zipfile
from pathlib import Path
from typing import List


def project_root() -> Path:
    """
    Retorna a raiz do projeto assumindo a estrutura:
    <raiz>/teste_1 e <raiz>/teste_2
    """
    return Path(__file__).resolve().parents[1]


ROOT_DIR = project_root()
OUTPUT_DIR = ROOT_DIR / "teste_2" / "output"

ZIP_NAME = "Teste_Whybid.zip"
ZIP_PATH = OUTPUT_DIR / ZIP_NAME


def files_to_pack() -> List[Path]:
    """
    Define quais arquivos entram no ZIP final.
    """
    candidates = [
        OUTPUT_DIR / "despesas_agregadas.csv"
    ]

    return [path for path in candidates if path.exists()]


def pack_output() -> Path:
    """
    Compacta os arquivos do output em Teste_Whybid.zip.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    selected_files = files_to_pack()
    if not selected_files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em {OUTPUT_DIR}. Rode o pipeline primeiro."
        )

    with zipfile.ZipFile(ZIP_PATH, mode="w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        for file_path in selected_files:
            zip_file.write(file_path, arcname=file_path.name)

    return ZIP_PATH
