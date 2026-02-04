from typing import Dict, List
import csv
import os

import pandas as pd


def normalize_header(header: str) -> str:
    """
    Normaliza nomes de colunas:
    - remove espaços extras
    - converte para maiúsculas
    """
    return header.strip().upper()


def read_csv_or_txt(file_path: str) -> List[Dict[str, str]]:
    """
    Lê arquivos CSV ou TXT delimitados por ponto e vírgula (;).
    """
    rows: List[Dict[str, str]] = []

    with open(file_path, mode="r", encoding="latin-1", newline="") as file:
        reader = csv.DictReader(file, delimiter=";")

        normalized_fieldnames = [
            normalize_header(field)
            for field in reader.fieldnames or []
        ]

        for raw_row in reader:
            normalized_row: Dict[str, str] = {}

            for original_key, normalized_key in zip(
                raw_row.keys(),
                normalized_fieldnames
            ):
                value = raw_row.get(original_key)
                normalized_row[normalized_key] = (
                    value.strip() if value else ""
                )

            rows.append(normalized_row)

    return rows


def read_xlsx(file_path: str) -> List[Dict[str, str]]:
    """
    Lê arquivos XLSX utilizando pandas e normaliza colunas.
    """
    df = pd.read_excel(file_path, dtype=str)

    df.columns = [
        normalize_header(column)
        for column in df.columns
    ]

    rows: List[Dict[str, str]] = []

    for _, row in df.iterrows():
        normalized_row: Dict[str, str] = {}

        for column in df.columns:
            value = row[column]
            normalized_row[column] = (
                str(value).strip()
                if pd.notna(value)
                else ""
            )

        rows.append(normalized_row)

    return rows


def read_file(file_path: str) -> List[Dict[str, str]]:
    """
    Detecta automaticamente o tipo do arquivo e faz a leitura.
    """
    extension = os.path.splitext(file_path)[1].lower()

    if extension in {".csv", ".txt"}:
        return read_csv_or_txt(file_path)

    if extension == ".xlsx":
        return read_xlsx(file_path)

    return []
