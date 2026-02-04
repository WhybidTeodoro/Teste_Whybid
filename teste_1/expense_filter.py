from typing import Dict, List, Optional

TARGET_DESCRIPTION = "DESPESAS COM EVENTOS / SINISTROS"


def parse_monetary_value(raw_value: str) -> Optional[float]:
    """
    Converte um valor monetário em string para float positivo.
    Retorna None se inválido ou <= 0.
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

    return value if value > 0 else None


def filter_expense_rows(
    rows: List[Dict[str, str]],
    year: int,
    quarter: int
) -> List[Dict[str, object]]:
    """
    Filtra apenas registros de 'Despesas com Eventos / Sinistros'
    e extrai os campos para consolidação.

    IMPORTANTE:
    Os dados contábeis têm REG_ANS (chave), não têm CNPJ/RazaoSocial.
    """
    filtered: List[Dict[str, object]] = []

    for row in rows:
        description = row.get("DESCRICAO", "").strip().upper()

        if description != TARGET_DESCRIPTION:
            continue

        value = parse_monetary_value(row.get("VL_SALDO_FINAL", ""))

        if value is None:
            continue

        filtered.append({
            "REG_ANS": row.get("REG_ANS", "").strip(),
            "CNPJ": "",
            "RazaoSocial": "",
            "Ano": year,
            "Trimestre": quarter,
            "ValorDespesas": value
        })

    return filtered
