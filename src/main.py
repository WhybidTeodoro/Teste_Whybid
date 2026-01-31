import os
import requests
import zipfile
from bs4 import BeautifulSoup
from typing import List, Tuple

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
BASE_DOWNLOAD_DIR = "W:/Projetos/Teste_Whybid/data/raw/zips"
BASE_EXTRACT_DIR = os.path.join(os.path.dirname(os.path.dirname((os.path.abspath(__file__)))),
                                "data",
                                "raw",
                                "extracted")


def get_links(url: str) -> List[str]:
    """
    Retorna todos os links (arquivos e pastas) de uma URL HTML.
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
    Descobre todos os trimestres disponíveis com base nos arquivos ZIP.
    Retorna uma lista de (ano, trimestre).
    """
    years = get_links(BASE_URL)
    trimesters = []

    for year in years:
        # Garante que é uma pasta de ano (ex: "2025/")
        if not year.endswith("/") or not year.strip("/").isdigit():
            continue

        year_number = int(year.strip("/"))
        year_url = f"{BASE_URL}{year}"

        files = get_links(year_url)

        for file in files:
            if file.endswith(".zip") and file[0].isdigit() and "T" in file:
                try:
                    trimestre = int(file[0])
                    trimesters.append((year_number, trimestre))
                except ValueError:
                    continue

    return trimesters


def get_last_three_trimesters() -> List[Tuple[int, int]]:
    trimesters = discover_available_trimesters()

    trimesters.sort(key=lambda x: (x[0], x[1]), reverse=True)

    return trimesters[:3]


def ensure_directory_exists(path: str):
    """
    Cria o diretório caso ele não exista.
    """
    if not os.path.exists(path):
        os.makedirs(path)


def download_last_three_trimesters():
    """
    Faz o download dos arquivos ZIP dos 3 últimos trimestres.
    """
    ensure_directory_exists(BASE_DOWNLOAD_DIR)

    trimesters = get_last_three_trimesters()

    if not trimesters:
        print("Nenhum trimestre encontrado.")
        return

    for year, quarter in trimesters:
        filename = f"{quarter}T{year}.zip"
        file_url = f"{BASE_URL}{year}/{filename}"
        local_path = os.path.join(BASE_DOWNLOAD_DIR, filename)

        if os.path.exists(local_path):
            print(f"Arquivo já existe, pulando download: {filename}")
            continue

        print(f"Baixando {filename}...")

        response = requests.get(file_url, stream=True)
        response.raise_for_status()

        with open(local_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        print(f"Download concluído: {filename}")


def extract_zip_files():
    """
    Extrai todos os arquivos ZIP da pasta de downloads para a pasta extracted
    """
    ensure_directory_exists(BASE_EXTRACT_DIR)

    zip_files = [
        file for file in os.listdir(BASE_DOWNLOAD_DIR)
        if file.endswith(".zip")
    ]

    if not zip_files:
        print("Nenhum arquivo Zip encontrado.")
        return

    for zip_name in zip_files:
        zip_path = os.path.join(BASE_DOWNLOAD_DIR, zip_name)
        extract_folder_name = zip_name.replace(".zip", "")
        extract_path = os.path.join(BASE_EXTRACT_DIR, extract_folder_name)

        if os.path.exists(extract_path):
            print(f"ZIP já extraido, pulando : {zip_name}")
            continue

        print(f"Extraindo {zip_name}")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)

        print(f"Extração concluida: {zip_name}")

if __name__ == "__main__":
    print("Iniciando download dos últimos 3 trimestres...")
    download_last_three_trimesters()
    print("Iniciando a extração dos arquivos ZIP...")
    extract_zip_files()
    print("Processo finalizado.")
