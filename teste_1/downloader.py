from typing import List
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
