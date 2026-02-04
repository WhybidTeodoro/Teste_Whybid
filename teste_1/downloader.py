from typing import Dict, List, Tuple
import os
import re
import requests

from utils import list_links

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
BASE_DOWNLOAD_DIR = "data/raw/zips"


ZIP_PATTERN = re.compile(r"([1-4])T(20\d{2})\.zip", re.IGNORECASE)


def get_available_years() -> List[int]:
    """
    Lista os anos disponíveis na base da ANS.
    """
    links = list_links(BASE_URL)

    years: List[int] = []

    for link in links:
        if link.endswith("/") and link.strip("/").isdigit():
            years.append(int(link.strip("/")))

    return sorted(years)


def get_zip_files_for_year(year: int) -> Dict[Tuple[int, int], List[str]]:
    """
    Retorna todos os arquivos ZIP de um ano,
    organizados por (ano, trimestre).
    """
    year_url = f"{BASE_URL}{year}/"
    links = list_links(year_url)

    result: Dict[Tuple[int, int], List[str]] = {}

    for link in links:
        match = ZIP_PATTERN.match(link)

        if not match:
            continue

        quarter = int(match.group(1))
        year_from_file = int(match.group(2))

        full_url = f"{year_url}{link}"
        key = (year_from_file, quarter)

        result.setdefault(key, []).append(full_url)

    return result


def get_last_three_trimesters_with_zips() -> Dict[Tuple[int, int], List[str]]:
    """
    Retorna os ZIPs dos 3 trimestres mais recentes disponíveis.
    """
    years = get_available_years()
    all_trimesters: Dict[Tuple[int, int], List[str]] = {}

    for year in years:
        year_data = get_zip_files_for_year(year)
        all_trimesters.update(year_data)

    sorted_trimesters = sorted(
        all_trimesters.keys(),
        key=lambda item: (item[0], item[1]),
        reverse=True
    )

    last_three = sorted_trimesters[:3]

    return {key: all_trimesters[key] for key in last_three}


def ensure_directory(path: str) -> None:
    """
    Garante que um diretório exista.
    """
    os.makedirs(path, exist_ok=True)


def download_zip_files(
    trimesters: Dict[Tuple[int, int], List[str]]
) -> List[Tuple[int, int, str]]:
    """
    Faz o download dos ZIPs informados.
    """
    downloaded_files: List[Tuple[int, int, str]] = []

    ensure_directory(BASE_DOWNLOAD_DIR)

    for (year, quarter), urls in trimesters.items():
        quarter_dir = os.path.join(BASE_DOWNLOAD_DIR, f"{year}_{quarter}T")
        ensure_directory(quarter_dir)

        for url in urls:
            filename = os.path.basename(url)
            local_path = os.path.join(quarter_dir, filename)

            if os.path.exists(local_path):
                downloaded_files.append((year, quarter, local_path))
                continue

            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            with open(local_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)

            downloaded_files.append((year, quarter, local_path))

    return downloaded_files
