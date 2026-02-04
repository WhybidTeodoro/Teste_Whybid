from __future__ import annotations

import csv
import statistics
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

CSV_INPUT = OUTPUT_DIR / "despesas_validadas.csv"
CSV_OUTPUT = OUTPUT_DIR / "despesas_agregadas.csv"

DELIMITER = ";"


def ensure_output_dir() -> None:
    """
    Garante que a pasta de saída exista.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def safe_str(value: str) -> str:
    """
    Evita None e remove espaços extras.
    """
    return (value or "").strip()


def parse_positive_float(value: str) -> Optional[float]:
    """
    Converte string para float positivo.
    Aceita formatos como '1234.56' e '1.234,56'.

    Retorna None se inválido ou <= 0.
    """
    raw = safe_str(value)
    if not raw:
        return None

    normalized = raw.replace(".", "").replace(",", ".")
    try:
        number = float(normalized)
    except ValueError:
        return None

    return number if number > 0 else None


def aggregate() -> None:
    """
    Agrupa por (RazaoSocial, UF) e calcula:
    - Total de despesas
    - Média por trimestre (baseada no total por trimestre)
    - Desvio padrão entre trimestres
    """
    ensure_output_dir()

    if not CSV_INPUT.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {CSV_INPUT}. Rode antes: python teste_2/validator.py"
        )

    # Estrutura:
    # groups[(RazaoSocial, UF)][(Ano, Trimestre)] = soma_do_trimestre
    groups: Dict[Tuple[str, str], Dict[Tuple[str, str], float]] = {}

    with CSV_INPUT.open(mode="r", encoding="utf-8", newline="") as fin:
        reader = csv.DictReader(fin, delimiter=DELIMITER)
        if not reader.fieldnames:
            raise ValueError("CSV de entrada não possui cabeçalho.")

        for row in reader:
            razao = safe_str(row.get("RazaoSocial", ""))
            uf = safe_str(row.get("UF", "Desconhecido")) or "Desconhecido"
            ano = safe_str(row.get("Ano", ""))
            trimestre = safe_str(row.get("Trimestre", ""))

            if not razao or not ano or not trimestre:
                continue

            valor = parse_positive_float(row.get("ValorDespesas", ""))
            if valor is None:
                continue

            group_key = (razao, uf)
            quarter_key = (ano, trimestre)

            groups.setdefault(group_key, {})
            groups[group_key][quarter_key] = groups[group_key].get(quarter_key, 0.0) + valor

    results: List[Dict[str, object]] = []

    for (razao, uf), quarter_map in groups.items():
        quarter_values = list(quarter_map.values())

        total = sum(quarter_values)
        media = statistics.mean(quarter_values) if quarter_values else 0.0

        if len(quarter_values) >= 2:
            desvio = statistics.pstdev(quarter_values)
        else:
            desvio = 0.0

        results.append({
            "RazaoSocial": razao,
            "UF": uf,
            "TotalDespesas": round(total, 2),
            "MediaDespesasPorTrimestre": round(media, 2),
            "DesvioPadraoDespesas": round(desvio, 2),
            "QtdTrimestres": len(quarter_values),
        })

    # Ordena pelo total (maior -> menor)
    results.sort(key=lambda item: float(item["TotalDespesas"]), reverse=True)

    fieldnames = [
        "RazaoSocial",
        "UF",
        "TotalDespesas",
        "MediaDespesasPorTrimestre",
        "DesvioPadraoDespesas",
        "QtdTrimestres",
    ]

    with CSV_OUTPUT.open(mode="w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames, delimiter=DELIMITER)
        writer.writeheader()
        writer.writerows(results)

    print("✅ Agregação concluída!")
    print(f"   ✔ Grupos (RazaoSocial + UF): {len(results)}")
    print(f"   ✔ CSV gerado: {CSV_OUTPUT}")


if __name__ == "__main__":
    aggregate()
