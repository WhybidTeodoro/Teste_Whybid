from typing import List, Tuple
import os
import zipfile
import shutil


BASE_ZIP_DIR = "data/raw/zips"
BASE_EXTRACT_DIR = "data/raw/extracted"


def ensure_directory(path: str) -> None:
    """
    Garante que um diretório exista.
    """
    os.makedirs(path, exist_ok=True)


def extract_zip(
    zip_path: str,
    destination_dir: str
) -> List[str]:
    """
    Extrai um arquivo ZIP para o diretório de destino.

    Retorna a lista de arquivos extraídos.
    """
    extracted_files: List[str] = []

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(destination_dir)
        extracted_files = [
            os.path.join(destination_dir, name)
            for name in zip_ref.namelist()
        ]

    return extracted_files


def extract_all_zips(
    downloaded_files: List[Tuple[int, int, str]]
) -> List[Tuple[int, int, str]]:
    """
    Extrai todos os ZIPs baixados, organizando por ano e trimestre.

    Retorna uma lista de tuplas:
    (ano, trimestre, caminho_arquivo_extraído)
    """
    ensure_directory(BASE_EXTRACT_DIR)

    extracted_results: List[Tuple[int, int, str]] = []

    for year, quarter, zip_path in downloaded_files:
        quarter_dir = os.path.join(BASE_EXTRACT_DIR, f"{year}_{quarter}T")
        ensure_directory(quarter_dir)

        extracted_files = extract_zip(zip_path, quarter_dir)

        for file_path in extracted_files:
            if os.path.isfile(file_path):
                extracted_results.append((year, quarter, file_path))

    return extracted_results


def clean_extracted_directory() -> None:
    """
    Remove completamente o diretório de arquivos extraídos.
    Útil para resetar o processamento.
    """
    if os.path.exists(BASE_EXTRACT_DIR):
        shutil.rmtree(BASE_EXTRACT_DIR)
