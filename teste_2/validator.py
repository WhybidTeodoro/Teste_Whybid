from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def project_root() -> Path:
    """
    Retorna a raiz do projeto assumindo a estrutura:
    <raiz>/teste_1 e <raiz>/teste_2
    """
    return Path(__file__).resolve().parents[1]


ROOT_DIR = project_root()

OUTPUT_DIR = ROOT_DIR / "teste_2" / "output"

CSV_INPUT = OUTPUT_DIR / "despesas_enriquecidas.csv"
CSV_VALIDATED = OUTPUT_DIR / "despesas_validadas.csv"
CSV_INVALID = OUTPUT_DIR / "registros_invalidos.csv"

DELIMITER = ";"


def ensure_output_dir() -> None:
    """
    Garante que a pasta de saída exista.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def only_digits(value: str) -> str:
    """
    Remove tudo que não for dígito.
    """
    return re.sub(r"\D", "", value or "")


def calculate_cnpj_digit(numbers: str, weights: List[int]) -> str:
    """
    Calcula um dígito verificador do CNPJ.
    """
    total = sum(int(n) * w for n, w in zip(numbers, weights))
    remainder = total % 11
    return "0" if remainder < 2 else str(11 - remainder)


def is_valid_cnpj(cnpj: str) -> bool:
    """
    Valida CNPJ:
    - deve ter 14 dígitos
    - não pode ser sequência repetida
    - valida dígitos verificadores
    """
    digits = only_digits(cnpj)

    if len(digits) != 14:
        return False

    if digits == digits[0] * 14:
        return False

    weights_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    weights_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    base_12 = digits[:12]
    digit_1 = calculate_cnpj_digit(base_12, weights_1)

    base_13 = digits[:12] + digit_1
    digit_2 = calculate_cnpj_digit(base_13, weights_2)

    return digits[-2:] == (digit_1 + digit_2)


def parse_positive_float(value: str) -> Optional[float]:
    """
    Converte string para float positivo.
    Aceita formatos como '1234.56' e '1.234,56'.

    Retorna None se inválido ou <= 0.
    """
    raw = (value or "").strip()
    if not raw:
        return None

    normalized = raw.replace(".", "").replace(",", ".")
    try:
        number = float(normalized)
    except ValueError:
        return None

    return number if number > 0 else None


def validate_row(row: Dict[str, str]) -> Tuple[bool, List[str]]:
    """
    Valida uma linha e retorna:
    (valido, lista_de_motivos)
    """
    reasons: List[str] = []

    cnpj = (row.get("CNPJ", "") or "").strip()
    if not is_valid_cnpj(cnpj):
        reasons.append("CNPJ_INVALIDO")

    razao = (row.get("RazaoSocial", "") or "").strip()
    if not razao:
        reasons.append("RAZAO_SOCIAL_VAZIA")

    valor = parse_positive_float(row.get("ValorDespesas", ""))
    if valor is None:
        reasons.append("VALOR_INVALIDO_OU_NAO_POSITIVO")

    # Se não teve match no cadastro, provavelmente UF/Modalidade ficam "Desconhecido"
    # Isso NÃO invalida o registro, pois o enunciado pede validar CNPJ/valor/razao.

    return (len(reasons) == 0), reasons


def validate_csv() -> None:
    """
    Lê o CSV enriquecido e gera:
    - despesas_validadas.csv (somente válidos)
    - registros_invalidos.csv (relatório com motivo)
    """
    ensure_output_dir()

    if not CSV_INPUT.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {CSV_INPUT}. Rode antes: python teste_2/enricher.py"
        )

    valid_rows: List[Dict[str, str]] = []
    invalid_rows: List[Dict[str, str]] = []

    with CSV_INPUT.open(mode="r", encoding="utf-8", newline="") as fin:
        reader = csv.DictReader(fin, delimiter=DELIMITER)

        if not reader.fieldnames:
            raise ValueError("CSV de entrada não possui cabeçalho.")

        input_fields = list(reader.fieldnames)
        invalid_fields = input_fields + ["Motivos"]

        for row in reader:
            is_valid, reasons = validate_row(row)

            if is_valid:
                valid_rows.append(row)
            else:
                row_copy = dict(row)
                row_copy["Motivos"] = ",".join(reasons)
                invalid_rows.append(row_copy)

    with CSV_VALIDATED.open(mode="w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=input_fields, delimiter=DELIMITER)
        writer.writeheader()
        writer.writerows(valid_rows)

    with CSV_INVALID.open(mode="w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=invalid_fields, delimiter=DELIMITER)
        writer.writeheader()
        writer.writerows(invalid_rows)

    print("✅ Validação concluída!")
    print(f"   ✔ Válidos: {len(valid_rows)} -> {CSV_VALIDATED}")
    print(f"   ✔ Inválidos: {len(invalid_rows)} -> {CSV_INVALID}")

