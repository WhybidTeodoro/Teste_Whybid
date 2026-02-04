from __future__ import annotations

from pathlib import Path

from enricher import run_enrichment
from validator import validate_csv
from aggregator import aggregate
from packer import pack_output


def project_root() -> Path:
    """
    Retorna a raiz do projeto assumindo a estrutura:
    <raiz>/teste_1 e <raiz>/teste_2
    """
    return Path(__file__).resolve().parents[1]


def main() -> None:
    """
    Orquestra o Teste 2 na ordem correta:
    1) Enriquecimento por REG_ANS
    2) Validação (CNPJ, valor, razão social)
    3) Agregação (total, média, desvio padrão)
    4) Empacotamento ZIP final
    """
    root = project_root()

    print("=" * 60)
    print("🚀 Iniciando TESTE 2 — Transformação e Validação de Dados")
    print("=" * 60)
    print(f"📁 Raiz do projeto: {root}")
    print()

    print("🔹 PASSO 1/4 — Enriquecimento (CADOP) + join por REG_ANS")
    run_enrichment()
    print("✅ PASSO 1 finalizado.")
    print()

    print("🔹 PASSO 2/4 — Validação (CNPJ, Razão Social, Valor > 0)")
    validate_csv()
    print("✅ PASSO 2 finalizado.")
    print()

    print("🔹 PASSO 3/4 — Agregação (total, média por trimestre, desvio padrão)")
    aggregate()
    print("✅ PASSO 3 finalizado.")
    print()

    print("🔹 PASSO 4/4 — Gerando ZIP final (Teste_Whybid.zip)")
    zip_path = pack_output()
    print(f"✅ PASSO 4 finalizado. ZIP gerado em: {zip_path}")
    print()

    print("=" * 60)
    print("🎉 TESTE 2 concluído com sucesso!")
    print("=" * 60)


if __name__ == "__main__":
    main()
