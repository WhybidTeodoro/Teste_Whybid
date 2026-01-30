import requests
from bs4 import BeautifulSoup
from typing import List, Tuple

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

def get_links(url: str) -> List[str]:
    """
    Retorna todos os links (arquivos e pastas) de uma URL.
    """

    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    links = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if href:
            links.append(href)

    return links

def discover_available_trimesters() -> List[Tuple[int, int]]:
    """
    Retorna uma lista de (ano, trimestre) encontrados nos nomes dos arquivos ZIP.
    """

    years = get_links(BASE_URL)

    trimesters = []

    for year in years:
        if not year.endswith("/") or not year.strip("/").isdigit():
            continue

        year_number = int(year.strip("/"))
        year_url = f"{BASE_URL}{year}"

        files = get_links(year_url)

        for file in files:
            if file.endswith(".zip") and "T" in file:
                trimester = int(file[0])
                trimesters.append((year_number, trimester))

    return trimesters

def get_last_three_trimesters() -> List[Tuple[int, int]]:
    trimesters = discover_available_trimesters()

    trimesters.sort(key=lambda x: (x[0], x[1]), reverse=True)

    return trimesters[:3]

if __name__ == "__main__":
    last_trimesters = get_last_three_trimesters()
    print("Últimos 3 trimestres encontrados:")
    for year, trimester in last_trimesters:
        print(f"Ano: {year}, Trimestre: {trimester}")
