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

CSV_INPUT = ROOT_DIR / "teste_1" / "output" / "despesas_eventos_sinistros.csv"
OUTPUT_DIR = ROOT_DIR / "teste_2" / "output"

CSV_VALIDATED = OUTPUT_DIR / "despesas_validadas.csv"
CSV_INVALID_CNPJS = OUTPUT_DIR / "cnpjs_invalidos.csv"

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
    Calcula um dígito verificador de CNPJ dado um conjunto de pesos.
    """
    total = sum(int(n) * w for n, w in zip(numbers, weights))
    remainder = total % 11
    return "0" if remainder < 2 else str(11 - remainder)


def is_valid_cnpj(cnpj: str) -> bool:
    """
    Valida um CNPJ:
    - 14 dígitos
    - não pode ser sequência repetida (ex: 000...0)
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

    return digits[-2:] == digit_1 + digit_2


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
    Valida uma linha
    """
    reasons: List[str] = []

    cnpj = row.get("CNPJ", "")
    if not is_valid_cnpj(cnpj):
        reasons.append("CNPJ_INVALIDO")

    razao = (row.get("RazaoSocial", "") or "").strip()
    if not razao:
        reasons.append("RAZAO_SOCIAL_VAZIA")

    valor = parse_positive_float(row.get("ValorDespesas", ""))
    if valor is None:
        reasons.append("VALOR_INVALIDO_OU_NAO_POSITIVO")

    return (len(reasons) == 0), reasons


def validate_csv() -> None:
    """
    Lê o CSV do Teste 1, valida registros
    """
    ensure_output_dir()

    if not CSV_INPUT.exists():
        raise FileNotFoundError(
            f"CSV de entrada não encontrado: {CSV_INPUT}. "
            "Execute o Teste 1 primeiro."
        )

    valid_rows: List[Dict[str, str]] = []
    invalid_cnpj_rows: List[Dict[str, str]] = []

    with CSV_INPUT.open(mode="r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file, delimiter=DELIMITER)

        if not reader.fieldnames:
            raise ValueError("CSV de entrada não possui cabeçalho.")

        fieldnames = list(reader.fieldnames)

        for row in reader:
            is_valid, reasons = validate_row(row)

            if is_valid:
                valid_rows.append(row)
            else:
                if "CNPJ_INVALIDO" in reasons:
                    invalid_cnpj_rows.append({
                        "CNPJ": row.get("CNPJ", ""),
                        "RazaoSocial": row.get("RazaoSocial", ""),
                        "Ano": row.get("Ano", ""),
                        "Trimestre": row.get("Trimestre", ""),
                        "ValorDespesas": row.get("ValorDespesas", ""),
                        "Motivos": ",".join(reasons),
                    })

    with CSV_VALIDATED.open(mode="w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=DELIMITER)
        writer.writeheader()
        writer.writerows(valid_rows)

    with CSV_INVALID_CNPJS.open(mode="w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["CNPJ", "RazaoSocial", "Ano", "Trimestre", "ValorDespesas", "Motivos"],
            delimiter=DELIMITER,
        )
        writer.writeheader()
        writer.writerows(invalid_cnpj_rows)

    print("✅ Validação concluída!")
    print(f"   ✔ Registros válidos: {len(valid_rows)} -> {CSV_VALIDATED}")
    print(f"   ✔ Relatório CNPJs inválidos: {len(invalid_cnpj_rows)} -> {CSV_INVALID_CNPJS}")


if __name__ == "__main__":
    validate_csv()
