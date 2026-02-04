from typing import Dict, List, Tuple
import re

from utils import list_links

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"


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

    return {
        key: all_trimesters[key]
        for key in last_three
    }
