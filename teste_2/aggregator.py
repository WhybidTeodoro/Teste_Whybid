from __future__ import annotations

import csv
import statistics
from pathlib import Path
from typing import Dict, List, Tuple, Optional


def project_root() -> Path:
    """
    Retorna a raiz do projeto assumindo a estrutura:
    <raiz>/teste_1 e <raiz>/teste_2
    """
    return Path(__file__).resolve().parents[1]


ROOT_DIR = project_root()

OUTPUT_DIR = ROOT_DIR / "teste_2" / "output"
CSV_ENRICHED = OUTPUT_DIR / "despesas_enriquecidas.csv"
CSV_AGGREGATED = OUTPUT_DIR / "despesas_agregadas.csv"

DELIMITER = ";"


def ensure_output_dir() -> None:
    """
    Garante que a pasta de saída exista.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


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


def safe_str(value: str) -> str:
    """
    Evita None e remove espaços extras.
    """
    return (value or "").strip()


def aggregate_expenses() -> None:
    """
    Agrupa por (RazaoSocial, UF) e calcula:
    - Total de despesas
    - Média por trimestre (com base no total por trimestre)
    - Desvio padrão entre trimestres

    Ordena por total (desc) e gera despesas_agregadas.csv.
    """
    ensure_output_dir()

    if not CSV_ENRICHED.exists():
        raise FileNotFoundError(
            f"CSV enriquecido não encontrado: {CSV_ENRICHED}. "
            "Rode primeiro: python teste_2/enricher.py"
        )

    quarterly_totals: Dict[Tuple[str, str], Dict[Tuple[str, str], float]] = {}

    with CSV_ENRICHED.open(mode="r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file, delimiter=DELIMITER)

        if not reader.fieldnames:
            raise ValueError("CSV enriquecido não possui cabeçalho.")

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

            quarterly_totals.setdefault(group_key, {})
            quarterly_totals[group_key][quarter_key] = (
                quarterly_totals[group_key].get(quarter_key, 0.0) + valor
            )


    results: List[Dict[str, object]] = []

    for (razao, uf), quarter_map in quarterly_totals.items():
        quarter_values = list(quarter_map.values())

        total = sum(quarter_values)
        mean = statistics.mean(quarter_values) if quarter_values else 0.0


        if len(quarter_values) >= 2:
            std_dev = statistics.pstdev(quarter_values)
        else:
            std_dev = 0.0

        results.append({
            "RazaoSocial": razao,
            "UF": uf,
            "TotalDespesas": round(total, 2),
            "MediaDespesasPorTrimestre": round(mean, 2),
            "DesvioPadraoDespesas": round(std_dev, 2),
            "QtdTrimestres": len(quarter_values),
        })


    results.sort(key=lambda item: float(item["TotalDespesas"]), reverse=True)


    fieldnames = [
        "RazaoSocial",
        "UF",
        "TotalDespesas",
        "MediaDespesasPorTrimestre",
        "DesvioPadraoDespesas",
        "QtdTrimestres",
    ]

    with CSV_AGGREGATED.open(mode="w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=DELIMITER)
        writer.writeheader()
        writer.writerows(results)

    print("✅ Agregação concluída!")
    print(f"   ✔ Grupos (RazaoSocial + UF): {len(results)}")
    print(f"   ✔ CSV gerado: {CSV_AGGREGATED}")


if __name__ == "__main__":
    aggregate_expenses()
