from __future__ import annotations

import csv
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urljoin

import requests

from utils import list_links


def project_root() -> Path:
    """
    Retorna a raiz do projeto assumindo a estrutura:
    <raiz>/teste_1 e <raiz>/teste_2
    """
    return Path(__file__).resolve().parents[1]


ROOT_DIR = project_root()

OUTPUT_DIR = ROOT_DIR / "teste_2" / "output"
DATA_DIR = ROOT_DIR / "teste_2" / "data" / "operadoras"

CSV_VALIDATED = OUTPUT_DIR / "despesas_validadas.csv"
CSV_ENRICHED = OUTPUT_DIR / "despesas_enriquecidas.csv"
CSV_DUPLICATES = OUTPUT_DIR / "operadoras_cnpj_duplicado.csv"

OPERADORAS_BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"


def ensure_dirs() -> None:
    """
    Cria pastas necessárias.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def normalize_header(text: str) -> str:
    """
    Normaliza cabeçalhos para comparar de forma consistente.
    """
    return (text or "").strip().upper()


def only_digits(value: str) -> str:
    """
    Remove tudo que não for dígito.
    """
    return re.sub(r"\D", "", value or "")


def detect_delimiter(file_path: Path) -> str:
    """
    Tenta detectar o delimitador do CSV (ex: ';' ou TAB).
    Mantém simples: usa csv.Sniffer e fallback para ';'.
    """
    sample = file_path.read_text(encoding="latin-1", errors="ignore")[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[";", "\t", ","])
        return dialect.delimiter
    except csv.Error:
        return ";"


def parse_cnpj_from_operadoras(raw: str) -> str:
    """
    Converte o CNPJ do cadastro para 14 dígitos.

    Casos:
    - '12345678000199' -> ok
    - '1,95419E+13' -> converte para inteiro e pad com zeros à esquerda
    """
    text = (raw or "").strip()

    # Se já tiver muitos dígitos, tenta só limpar
    digits = only_digits(text)
    if len(digits) == 14:
        return digits

    # Tenta lidar com notação científica (ex: 1,95419E+13)
    # Troca vírgula por ponto e usa Decimal para evitar float
    try:
        dec = Decimal(text.replace(",", "."))
        as_int = int(dec.to_integral_value(rounding="ROUND_HALF_UP"))
        return str(as_int).zfill(14)
    except (InvalidOperation, ValueError):
        # Última tentativa: só limpar dígitos e pad se der
        if digits:
            return digits.zfill(14)
        return ""


def download_operadoras_csv() -> Path:
    """
    Baixa o CSV de operadoras ativas (Relatorio_cadop.csv).
    """
    ensure_dirs()

    links = list_links(OPERADORAS_BASE_URL)
    csv_links = sorted([link for link in links if link.lower().endswith(".csv")])

    if not csv_links:
        raise RuntimeError("Nenhum CSV encontrado na pasta de operadoras ativas.")

    filename = csv_links[-1]
    url = urljoin(OPERADORAS_BASE_URL, filename)
    local_path = DATA_DIR / filename

    if local_path.exists():
        return local_path

    print(f"⬇️  Baixando cadastro de operadoras: {filename}")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    with local_path.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    return local_path


def pick_first(row: Dict[str, str], keys: List[str]) -> str:
    """
    Retorna o primeiro valor não vazio para as chaves informadas.
    """
    for k in keys:
        value = (row.get(k, "") or "").strip()
        if value:
            return value
    return ""


def load_operadoras_map(operadoras_csv: Path) -> Tuple[Dict[str, Dict[str, str]], List[Dict[str, str]]]:
    """
    Carrega o cadastro em um dicionário:
    cnpj_14_digitos -> {RegistroANS, Modalidade, UF}

    Também gera um relatório simples de duplicados divergentes.
    """
    delimiter = detect_delimiter(operadoras_csv)

    operadoras_map: Dict[str, Dict[str, str]] = {}
    duplicates: List[Dict[str, str]] = []

    with operadoras_csv.open(mode="r", encoding="latin-1", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)

        if not reader.fieldnames:
            raise ValueError("CSV de operadoras não possui cabeçalho.")

        normalized_headers = [normalize_header(h) for h in reader.fieldnames]

        for raw_row in reader:
            row: Dict[str, str] = {}
            for original_key, normalized_key in zip(raw_row.keys(), normalized_headers):
                row[normalized_key] = (raw_row.get(original_key, "") or "").strip()

            cnpj = parse_cnpj_from_operadoras(row.get("CNPJ", ""))
            if not cnpj:
                continue

            registro_ans = pick_first(row, ["REGISTRO_OPERADORA", "REGISTROANS", "REGISTRO_ANS", "REG_ANS"])
            modalidade = pick_first(row, ["MODALIDADE"])
            uf = pick_first(row, ["UF"])

            payload = {"RegistroANS": registro_ans, "Modalidade": modalidade, "UF": uf}

            if cnpj not in operadoras_map:
                operadoras_map[cnpj] = payload
                continue

            if operadoras_map[cnpj] != payload:
                prev = operadoras_map[cnpj]
                duplicates.append({
                    "CNPJ": cnpj,
                    "RegistroANS_1": prev.get("RegistroANS", ""),
                    "Modalidade_1": prev.get("Modalidade", ""),
                    "UF_1": prev.get("UF", ""),
                    "RegistroANS_2": payload.get("RegistroANS", ""),
                    "Modalidade_2": payload.get("Modalidade", ""),
                    "UF_2": payload.get("UF", ""),
                })

    return operadoras_map, duplicates


def write_duplicates_report(rows: List[Dict[str, str]]) -> None:
    """
    Salva relatório de CNPJs duplicados divergentes no cadastro.
    """
    if not rows:
        return

    with CSV_DUPLICATES.open(mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "CNPJ",
                "RegistroANS_1", "Modalidade_1", "UF_1",
                "RegistroANS_2", "Modalidade_2", "UF_2",
            ],
            delimiter=";",
        )
        writer.writeheader()
        writer.writerows(rows)


def enrich_validated_csv(operadoras_map: Dict[str, Dict[str, str]]) -> None:
    """
    Faz um left join no CSV validado, adicionando:
    RegistroANS, Modalidade, UF
    """
    if not CSV_VALIDATED.exists():
        raise FileNotFoundError(
            f"CSV validado não encontrado: {CSV_VALIDATED}. Rode: python teste_2/validator.py"
        )

    with CSV_VALIDATED.open(mode="r", encoding="utf-8", newline="") as fin:
        reader = csv.DictReader(fin, delimiter=";")

        if not reader.fieldnames:
            raise ValueError("CSV validado não possui cabeçalho.")

        input_fields = list(reader.fieldnames)
        output_fields = input_fields + ["RegistroANS", "Modalidade", "UF"]

        with CSV_ENRICHED.open(mode="w", encoding="utf-8", newline="") as fout:
            writer = csv.DictWriter(fout, fieldnames=output_fields, delimiter=";")
            writer.writeheader()

            match = 0
            no_match = 0

            for row in reader:
                cnpj = only_digits(row.get("CNPJ", ""))
                cadastro = operadoras_map.get(cnpj)

                if cadastro:
                    row["RegistroANS"] = cadastro.get("RegistroANS") or "Desconhecido"
                    row["Modalidade"] = cadastro.get("Modalidade") or "Desconhecido"
                    row["UF"] = cadastro.get("UF") or "Desconhecido"
                    match += 1
                else:
                    row["RegistroANS"] = "Desconhecido"
                    row["Modalidade"] = "Desconhecido"
                    row["UF"] = "Desconhecido"
                    no_match += 1

                writer.writerow(row)

    print("✅ Enriquecimento concluído!")
    print(f"   ✔ Match no cadastro: {match}")
    print(f"   ✔ Sem match (Desconhecido): {no_match}")
    print(f"   ✔ CSV gerado: {CSV_ENRICHED}")


def run_enrichment() -> None:
    """
    Pipeline do passo 2.2:
    - baixar cadastro
    - carregar mapa por CNPJ
    - enriquecer despesas_validadas.csv
    """
    ensure_dirs()

    operadoras_csv = download_operadoras_csv()
    print(f"📄 Cadastro baixado: {operadoras_csv.name}")

    operadoras_map, duplicates = load_operadoras_map(operadoras_csv)
    print(f"📥 Operadoras carregadas (CNPJ únicos): {len(operadoras_map)}")

    if duplicates:
        write_duplicates_report(duplicates)
        print(f"⚠️  Duplicados divergentes: {len(duplicates)} -> {CSV_DUPLICATES}")

    enrich_validated_csv(operadoras_map)


if __name__ == "__main__":
    run_enrichment()
