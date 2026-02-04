from typing import Dict, List, Optional


TARGET_DESCRIPTION = "DESPESAS COM EVENTOS / SINISTROS"


def parse_monetary_value(raw_value: str) -> Optional[float]:
    """
    Converte um valor monetário em string para float.

    Retorna None se o valor for inválido.
    """
    if not raw_value:
        return None

    cleaned_value = (
        raw_value.replace(".", "")
        .replace(",", ".")
        .strip()
    )

    try:
        value = float(cleaned_value)
    except ValueError:
        return None

    if value <= 0:
        return None

    return value


def filter_expense_rows(
    rows: List[Dict[str, str]],
    year: int,
    quarter: int
) -> List[Dict[str, object]]:
    """
    Filtra apenas registros de 'Despesas com Eventos / Sinistros'
    e extrai os campos normalizados para consolidação.
    """
    filtered: List[Dict[str, object]] = []

    for row in rows:
        description = row.get("DESCRICAO", "").upper()

        if description != TARGET_DESCRIPTION:
            continue

        value = parse_monetary_value(row.get("VL_SALDO_FINAL", ""))

        if value is None:
            continue

        filtered.append({
            "CNPJ": row.get("CNPJ", "").strip(),
            "RazaoSocial": row.get("RAZAO_SOCIAL", "").strip(),
            "Ano": year,
            "Trimestre": quarter,
            "ValorDespesas": value
        })

    return filtered
