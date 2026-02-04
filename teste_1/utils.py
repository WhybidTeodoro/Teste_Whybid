from typing import List
import requests
from bs4 import BeautifulSoup


def list_links(url: str) -> List[str]:
    """
    Retorna todos os links encontrados em uma página HTML.
    Se a página não existir ou falhar, retorna lista vazia.
    """
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
    except requests.RequestException:
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    return [
        link.get("href")
        for link in soup.find_all("a")
        if link.get("href")
    ]
