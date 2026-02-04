from __future__ import annotations

import csv
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

# Entrada do Teste 1 (agora com REG_ANS preenchido)
CSV_INPUT = ROOT_DIR / "teste_1" / "output" / "despesas_eventos_sinistros.csv"

# Saídas do Teste 2
OUTPUT_DIR = ROOT_DIR / "teste_2" / "output"
CSV_ENRICHED = OUTPUT_DIR / "despesas_enriquecidas.csv"
CSV_NO_MATCH = OUTPUT_DIR / "reg_ans_sem_match.csv"

# Download do CADOP
DATA_DIR = ROOT_DIR / "teste_2" / "data" / "cadop"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CADOP_BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"


def ensure_dirs() -> None:
    """
    Garante que as pastas de saída existam.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def detect_delimiter(file_path: Path) -> str:
    """
    Detecta o delimitador do arquivo (ex: ';' ou TAB).
    Mantém simples com csv.Sniffer, com fallback para ';'.
    """
    sample = file_path.read_text(encoding="latin-1", errors="ignore")[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[";", "\t", ","])
        return dialect.delimiter
    except csv.Error:
        return ";"


def normalize_header(text: str) -> str:
    """
    Normaliza nomes de colunas para facilitar busca.
    """
    return (text or "").strip().upper()


def download_latest_cadop_csv() -> Path:
    """
    Baixa o CSV mais recente na pasta de operadoras ativas.
    """
    ensure_dirs()

    links = list_links(CADOP_BASE_URL)
    csv_links = sorted([link for link in links if link.lower().endswith(".csv")])

    if not csv_links:
        raise RuntimeError("Nenhum CSV encontrado na pasta de operadoras ativas (CADOP).")

    filename = csv_links[-1]
    url = urljoin(CADOP_BASE_URL, filename)
    local_path = DATA_DIR / filename

    if local_path.exists():
        return local_path

    print(f"⬇️  Baixando CADOP: {filename}")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    with local_path.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    return local_path


def parse_cnpj(raw: str) -> str:
    """
    Converte CNPJ do CADOP para 14 dígitos.
    Trata casos como '1,95419E+13' (notação científica).

    Retorna string com 14 dígitos, ou '' se não der para converter.
    """
    text = (raw or "").strip()
    if not text:
        return ""

    # Caso já esteja "limpo" (raríssimo no CADOP, mas ok)
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 14:
        return digits

    # Notação científica com vírgula (ex: 1,95419E+13)
    try:
        dec = Decimal(text.replace(",", "."))
        as_int = int(dec.to_integral_value(rounding="ROUND_HALF_UP"))
        return str(as_int).zfill(14)
    except (InvalidOperation, ValueError):
        return digits.zfill(14) if digits else ""


def load_cadop_map(cadop_csv: Path) -> Dict[str, Dict[str, str]]:
    """
    Carrega o cadastro em memória, criando um mapa:
    REGISTRO_OPERADORA -> {CNPJ, RazaoSocial, Modalidade, UF}

    Se houver REGISTRO_OPERADORA duplicado, mantém o primeiro (simples).
    """
    delimiter = detect_delimiter(cadop_csv)
    cadop_map: Dict[str, Dict[str, str]] = {}

    with cadop_csv.open(mode="r", encoding="latin-1", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        if not reader.fieldnames:
            raise ValueError("CADOP CSV não possui cabeçalho.")

        normalized_headers = [normalize_header(h) for h in reader.fieldnames]

        for raw_row in reader:
            row: Dict[str, str] = {}
            for original_key, norm_key in zip(raw_row.keys(), normalized_headers):
                row[norm_key] = (raw_row.get(original_key, "") or "").strip()

            registro = row.get("REGISTRO_OPERADORA", "").strip()
            if not registro:
                continue

            # Mantém o primeiro registro (trade-off simples)
            if registro in cadop_map:
                continue

            cnpj = parse_cnpj(row.get("CNPJ", ""))
            razao = row.get("RAZAO_SOCIAL", "").strip()
            modalidade = row.get("MODALIDADE", "").strip()
            uf = row.get("UF", "").strip()

            cadop_map[registro] = {
                "CNPJ": cnpj,
                "RazaoSocial": razao,
                "Modalidade": modalidade,
                "UF": uf,
                "RegistroANS": registro,
            }

    return cadop_map


def enrich_consolidated(cadop_map: Dict[str, Dict[str, str]]) -> None:
    """
    Faz join:
    REG_ANS (despesas do Teste 1) -> REGISTRO_OPERADORA (CADOP)

    Adiciona:
    RegistroANS, Modalidade, UF
    e também preenche CNPJ e RazaoSocial quando possível.
    """
    if not CSV_INPUT.exists():
        raise FileNotFoundError(f"CSV do Teste 1 não encontrado: {CSV_INPUT}")

    no_match_rows: List[Dict[str, str]] = []

    with CSV_INPUT.open(mode="r", encoding="utf-8", newline="") as fin:
        reader = csv.DictReader(fin, delimiter=";")
        if not reader.fieldnames:
            raise ValueError("CSV de entrada não possui cabeçalho.")

        input_fields = list(reader.fieldnames)

        # Garante que as colunas existam no output
        extra_fields = ["RegistroANS", "Modalidade", "UF"]
        output_fields = input_fields[:]

        for col in extra_fields:
            if col not in output_fields:
                output_fields.append(col)

        with CSV_ENRICHED.open(mode="w", encoding="utf-8", newline="") as fout:
            writer = csv.DictWriter(fout, fieldnames=output_fields, delimiter=";")
            writer.writeheader()

            match_count = 0
            no_match_count = 0

            for row in reader:
                reg_ans = (row.get("REG_ANS", "") or "").strip()
                cadastro = cadop_map.get(reg_ans)

                if cadastro:
                    # Preenche CNPJ e RazaoSocial se estiverem vazios
                    if not (row.get("CNPJ", "") or "").strip():
                        row["CNPJ"] = cadastro["CNPJ"]
                    if not (row.get("RazaoSocial", "") or "").strip():
                        row["RazaoSocial"] = cadastro["RazaoSocial"]

                    row["RegistroANS"] = cadastro["RegistroANS"] or "Desconhecido"
                    row["Modalidade"] = cadastro["Modalidade"] or "Desconhecido"
                    row["UF"] = cadastro["UF"] or "Desconhecido"
                    match_count += 1
                else:
                    row["RegistroANS"] = "Desconhecido"
                    row["Modalidade"] = "Desconhecido"
                    row["UF"] = "Desconhecido"
                    no_match_count += 1

                    no_match_rows.append({
                        "REG_ANS": reg_ans,
                        "Ano": (row.get("Ano", "") or "").strip(),
                        "Trimestre": (row.get("Trimestre", "") or "").strip(),
                    })

                writer.writerow(row)

    # Relatório simples de registros sem match
    if no_match_rows:
        with CSV_NO_MATCH.open(mode="w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["REG_ANS", "Ano", "Trimestre"], delimiter=";")
            writer.writeheader()
            writer.writerows(no_match_rows)

    print("✅ Enriquecimento concluído!")
    print(f"   ✔ Linhas com match: {match_count}")
    print(f"   ✔ Linhas sem match: {no_match_count}")
    print(f"   ✔ CSV gerado: {CSV_ENRICHED}")
    if no_match_rows:
        print(f"   ⚠ Relatório sem match: {CSV_NO_MATCH}")


def run_enrichment() -> None:
    """
    - baixa o CADOP
    - cria mapa por REGISTRO_OPERADORA
    - enriquece o consolidado usando REG_ANS
    """
    ensure_dirs()

    print("🔍 Baixando e lendo cadastro (CADOP)...")
    cadop_csv = download_latest_cadop_csv()

    print("📥 Carregando cadastro em memória...")
    cadop_map = load_cadop_map(cadop_csv)
    print(f"   ✔ Cadastros carregados: {len(cadop_map)}")

    print("🔗 Fazendo join por REG_ANS...")
    enrich_consolidated(cadop_map)

