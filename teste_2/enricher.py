from __future__ import annotations

import csv
import re
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

# Entradas/saídas do Teste 2
OUTPUT_DIR = ROOT_DIR / "teste_2" / "output"
CSV_VALIDATED = OUTPUT_DIR / "despesas_validadas.csv"
CSV_ENRICHED = OUTPUT_DIR / "despesas_enriquecidas.csv"

# Relatórios (para casos críticos)
CSV_DUPLICATE_OPERADORAS = OUTPUT_DIR / "operadoras_cnpj_duplicado.csv"

# Download do cadastro ANS
DATA_DIR = ROOT_DIR / "teste_2" / "data"
OPERADORAS_DIR = DATA_DIR / "operadoras"
OPERADORAS_CSV_LOCAL = OPERADORAS_DIR / "Relatorio_cadop.csv"

# URL base do cadastro de operadoras ativas (ANS)
OPERADORAS_BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"

DELIMITER = ";"


def ensure_dirs() -> None:
    """
    Garante que os diretórios de saída e dados existam.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OPERADORAS_DIR.mkdir(parents=True, exist_ok=True)


def only_digits(value: str) -> str:
    """
    Remove tudo que não for dígito (útil para padronizar CNPJ).
    """
    return re.sub(r"\D", "", value or "")


def download_operadoras_csv() -> Path:
    """
    Baixa o CSV de operadoras ativas (Relatorio_cadop.csv).

    Estratégia simples:
    - Lista links na pasta
    - Seleciona o CSV (se houver mais de um, pega o "último" por ordenação)
    - Faz download apenas se ainda não existir localmente
    """
    ensure_dirs()

    links = list_links(OPERADORAS_BASE_URL)
    csv_links = sorted([link for link in links if link.lower().endswith(".csv")])

    if not csv_links:
        raise RuntimeError("Nenhum arquivo CSV encontrado na pasta de operadoras ativas.")

    chosen_name = csv_links[-1]  # simples e suficiente aqui
    chosen_url = urljoin(OPERADORAS_BASE_URL, chosen_name)

    local_path = OPERADORAS_DIR / chosen_name

    if local_path.exists():
        return local_path

    print(f"⬇️  Baixando cadastro de operadoras: {chosen_name}")
    response = requests.get(chosen_url, stream=True, timeout=60)
    response.raise_for_status()

    with local_path.open("wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)

    return local_path


def normalize_header(header: str) -> str:
    """
    Normaliza cabeçalhos para facilitar matching de colunas.
    """
    return (header or "").strip().upper()


def pick_first_existing(row: Dict[str, str], keys: List[str]) -> str:
    """
    Retorna o primeiro valor não vazio encontrado na lista de chaves.
    Ajuda a lidar com pequenas variações no nome das colunas.
    """
    for key in keys:
        value = (row.get(key, "") or "").strip()
        if value:
            return value
    return ""


def load_operadoras_map(operadoras_csv: Path) -> Tuple[Dict[str, Dict[str, str]], List[Dict[str, str]]]:
    """
    Carrega o cadastro de operadoras em memória (é pequeno o suficiente).
    """
    operadoras_map: Dict[str, Dict[str, str]] = {}
    duplicates_report: List[Dict[str, str]] = []

    with operadoras_csv.open(mode="r", encoding="latin-1", newline="") as file:
        reader = csv.DictReader(file, delimiter=DELIMITER)

        if not reader.fieldnames:
            raise ValueError("CSV de operadoras não possui cabeçalho.")

        fieldnames = [normalize_header(h) for h in reader.fieldnames]
        # cria um leitor "normalizado" (re-mapeando chaves)
        for raw in reader:
            row: Dict[str, str] = {}
            for original_key, normalized_key in zip(raw.keys(), fieldnames):
                row[normalized_key] = (raw.get(original_key, "") or "").strip()

            cnpj = only_digits(row.get("CNPJ", ""))

            if not cnpj:
                continue

            registro_ans = pick_first_existing(row, ["REGISTROANS", "REGISTRO_ANS", "REG_ANS"])
            modalidade = pick_first_existing(row, ["MODALIDADE"])
            uf = pick_first_existing(row, ["UF"])

            payload = {
                "RegistroANS": registro_ans,
                "Modalidade": modalidade,
                "UF": uf,
            }

            if cnpj not in operadoras_map:
                operadoras_map[cnpj] = payload
                continue

            # Se já existe, registramos como duplicado caso haja divergência
            current = operadoras_map[cnpj]
            if payload != current:
                duplicates_report.append({
                    "CNPJ": cnpj,
                    "RegistroANS_1": current.get("RegistroANS", ""),
                    "Modalidade_1": current.get("Modalidade", ""),
                    "UF_1": current.get("UF", ""),
                    "RegistroANS_2": payload.get("RegistroANS", ""),
                    "Modalidade_2": payload.get("Modalidade", ""),
                    "UF_2": payload.get("UF", ""),
                })

    return operadoras_map, duplicates_report


def write_duplicates_report(rows: List[Dict[str, str]]) -> None:
    """
    Escreve relatório de CNPJs duplicados no cadastro de operadoras.
    """
    if not rows:
        return

    with CSV_DUPLICATE_OPERADORAS.open(mode="w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "CNPJ",
                "RegistroANS_1", "Modalidade_1", "UF_1",
                "RegistroANS_2", "Modalidade_2", "UF_2",
            ],
            delimiter=DELIMITER,
        )
        writer.writeheader()
        writer.writerows(rows)


def enrich_validated_csv(operadoras_map: Dict[str, Dict[str, str]]) -> None:
    """
    Faz join do CSV validado com o cadastro de operadoras.
    """
    if not CSV_VALIDATED.exists():
        raise FileNotFoundError(
            f"CSV validado não encontrado: {CSV_VALIDATED}. "
            "Rode primeiro: python teste_2/validator.py"
        )

    with CSV_VALIDATED.open(mode="r", encoding="utf-8", newline="") as file_in:
        reader = csv.DictReader(file_in, delimiter=DELIMITER)
        if not reader.fieldnames:
            raise ValueError("CSV validado não possui cabeçalho.")

        input_fields = list(reader.fieldnames)
        extra_fields = ["RegistroANS", "Modalidade", "UF"]
        output_fields = input_fields + extra_fields

        with CSV_ENRICHED.open(mode="w", encoding="utf-8", newline="") as file_out:
            writer = csv.DictWriter(file_out, fieldnames=output_fields, delimiter=DELIMITER)
            writer.writeheader()

            enriched_count = 0
            unknown_count = 0

            for row in reader:
                cnpj_digits = only_digits(row.get("CNPJ", ""))

                cadastro = operadoras_map.get(cnpj_digits)
                if cadastro:
                    row["RegistroANS"] = cadastro.get("RegistroANS") or "Desconhecido"
                    row["Modalidade"] = cadastro.get("Modalidade") or "Desconhecido"
                    row["UF"] = cadastro.get("UF") or "Desconhecido"
                    enriched_count += 1
                else:
                    row["RegistroANS"] = "Desconhecido"
                    row["Modalidade"] = "Desconhecido"
                    row["UF"] = "Desconhecido"
                    unknown_count += 1

                writer.writerow(row)

    print("✅ Enriquecimento concluído!")
    print(f"   ✔ Linhas com match no cadastro: {enriched_count}")
    print(f"   ✔ Linhas sem match (Desconhecido): {unknown_count}")
    print(f"   ✔ CSV gerado: {CSV_ENRICHED}")


def run_enrichment() -> None:
    """
    - download do cadastro
    - carga em memória (mapa por CNPJ)
    - geração do CSV enriquecido
    - relatório de duplicados
    """
    ensure_dirs()

    print("🔍 Obtendo cadastro de operadoras ativas (ANS)...")
    operadoras_csv = download_operadoras_csv()

    print("📥 Carregando cadastro em memória...")
    operadoras_map, duplicates_report = load_operadoras_map(operadoras_csv)

    print(f"   ✔ Operadoras carregadas (CNPJ únicos): {len(operadoras_map)}")

    if duplicates_report:
        write_duplicates_report(duplicates_report)
        print(f"   ⚠ CNPJs duplicados no cadastro: {len(duplicates_report)} -> {CSV_DUPLICATE_OPERADORAS}")
    else:
        print("   ✔ Nenhum CNPJ duplicado divergente encontrado no cadastro.")

    print("🔗 Fazendo join com despesas validadas...")
    enrich_validated_csv(operadoras_map)


if __name__ == "__main__":
    run_enrichment()
