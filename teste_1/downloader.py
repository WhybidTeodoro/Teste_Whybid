from typing import Dict, List, Tuple
import os
import re
import requests

from utils import list_links

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
BASE_DOWNLOAD_DIR = "data/raw/zips"


def get_available_years() -> List[int]:
    """
    Descobre os anos disponíveis na base da ANS.
    """
    links = list_links(BASE_URL)

    years: List[int] = []

    for link in links:
        if link.endswith("/") and link.strip("/").isdigit():
            years.append(int(link.strip("/")))

    return sorted(years)


def extract_year_quarter_from_text(text: str) -> Tuple[int, int] | None:
    """
    Extrai (ano, trimestre) a partir de um texto usando regex.
    """
    pattern = re.compile(r'([1-4])\s*T\s*[_-]?\s*(20\d{2})', re.IGNORECASE)
    match = pattern.search(text)

    if not match:
        return None

    quarter = int(match.group(1))
    year = int(match.group(2))

    return year, quarter


def find_zip_files_for_year(year: int) -> Dict[Tuple[int, int], List[str]]:
    """
    Navega recursivamente pela estrutura de um ano e encontra
    todos os arquivos ZIP associados a trimestres.
    """
    base_year_url = f"{BASE_URL}{year}/"
    result: Dict[Tuple[int, int], List[str]] = {}

    stack: List[str] = [base_year_url]

    while stack:
        current_url = stack.pop()
        links = list_links(current_url)

        for link in links:
            if link == "../":
                continue

            full_url = f"{current_url}{link}"

            if link.endswith("/"):
                stack.append(full_url)
                continue

            if not link.lower().endswith(".zip"):
                continue

            extracted = (
                extract_year_quarter_from_text(link)
                or extract_year_quarter_from_text(current_url)
            )

            if not extracted:
                continue

            year_q, quarter = extracted
            key = (year_q, quarter)

            result.setdefault(key, []).append(full_url)

    return result


def get_last_three_trimesters_with_zips() -> Dict[Tuple[int, int], List[str]]:
    """
    Retorna um dicionário contendo apenas os ZIPs
    dos 3 trimestres mais recentes disponíveis.
    """
    years = get_available_years()
    all_trimesters: Dict[Tuple[int, int], List[str]] = {}

    for year in years:
        year_data = find_zip_files_for_year(year)
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
    Faz o download incremental dos ZIPs dos trimestres informados.

    Retorna uma lista de tuplas:
    (ano, trimestre, caminho_local_zip)
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

            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()

            with open(local_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)

            downloaded_files.append((year, quarter, local_path))

    return downloaded_files
