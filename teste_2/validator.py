from typing import List, Dict, Tuple
import csv
import re
from pathlib import Path

CSV_INPUT = Path("teste_1/output/despesas_eventos_sinistros.csv")
CSV_OUTPUT = Path("teste_2/output/despesas_validadas.csv")


def validate_cnpj(cnpj: str) -> bool:
    """
    Valida CNPJ:
    - Remove caracteres não numéricos
    - Verifica se tem 14 dígitos
    - Valida os dígitos verificadores
    """
    cnpj_numbers = re.sub(r"\D", "", cnpj)

    if len(cnpj_numbers) != 14:
        return False

    # CNPJs com todos dígitos iguais são inválidos
    if cnpj_numbers == cnpj_numbers[0] * 14:
        return False

    def calc_digit(numbers: str) -> str:
        weights = [6,5,4,3,2,9,8,7,6,5,4,3,2]
        numbers = "0" + numbers if len(numbers) == 12 else numbers
        total = sum(int(n) * w for n, w in zip(numbers, weights[-len(numbers):]))
        remainder = total % 11
        return "0" if remainder < 2 else str(11 - remainder)

    digit1 = calc_digit(cnpj_numbers[:12])
    digit2 = calc_digit(cnpj_numbers[:13])

    return cnpj_numbers[-2:] == digit1 + digit2


def validate_row(row: Dict[str, str]) -> bool:
    """
    Valida cada linha do CSV:
    - CNPJ válido
    - ValorDespesas > 0
    - RazaoSocial não vazia
    """
    cnpj_ok = validate_cnpj(row.get("CNPJ", ""))
    valor_ok = float(row.get("ValorDespesas", 0)) > 0
    razao_ok = bool(row.get("RazaoSocial", "").strip())

    return cnpj_ok and valor_ok and razao_ok


def validate_csv(input_path: Path, output_path: Path) -> None:
    """
    Lê o CSV, valida cada linha e escreve apenas linhas válidas
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    valid_rows: List[Dict[str, str]] = []

    with input_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            if validate_row(row):
                valid_rows.append(row)

    if not valid_rows:
        print("Nenhuma linha válida encontrada!")
        return

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(valid_rows)

    print(f"Validação concluída. Linhas válidas salvas em {output_path}")


if __name__ == "__main__":
    validate_csv(CSV_INPUT, CSV_OUTPUT)
