from typing import Dict, List

from downloader import (
    get_last_three_trimesters_with_zips,
    download_zip_files,
)
from extractor import extract_all_zips
from file_reader import read_file
from expense_filter import filter_expense_rows
from consolidator import write_csv, zip_result


def main() -> None:
    """
    - Descoberta dos últimos 3 trimestres
    - Download dos ZIPs
    - Extração dos arquivos
    - Leitura automática (CSV / TXT / XLSX)
    - Filtro de despesas com eventos / sinistros
    - Consolidação em CSV
    - Compactação em ZIP
    """


    print("🔍 Buscando os últimos 3 trimestres disponíveis...")
    trimesters_with_zips = get_last_three_trimesters_with_zips()
    print(f"   ✔ Trimestres encontrados: {list(trimesters_with_zips.keys())}\n")


    print("⬇️  Baixando arquivos ZIP...")
    downloaded_zips = download_zip_files(trimesters_with_zips)
    print(f"   ✔ Total de ZIPs baixados: {len(downloaded_zips)}\n")


    print("📦 Extraindo arquivos ZIP...")
    extracted_files = extract_all_zips(downloaded_zips)
    print(f"   ✔ Total de arquivos extraídos: {len(extracted_files)}\n")


    print("🧹 Processando arquivos e filtrando despesas com eventos/sinistros...")
    consolidated_rows: List[Dict[str, object]] = []

    for year, quarter, file_path in extracted_files:
        print(f"   📄 Lendo arquivo: {file_path}")

        rows = read_file(file_path)

        if not rows:
            print("      ⚠ Arquivo ignorado (formato não suportado ou vazio)")
            continue

        filtered_rows = filter_expense_rows(
            rows=rows,
            year=year,
            quarter=quarter
        )

        print(f"      ✔ Registros válidos encontrados: {len(filtered_rows)}")
        consolidated_rows.extend(filtered_rows)

    print(f"\n   ✔ Total de registros consolidados: {len(consolidated_rows)}\n")


    print("📝 Gerando CSV consolidado...")
    csv_path = write_csv(consolidated_rows)
    print(f"   ✔ CSV gerado em: {csv_path}\n")


    print("🗜️  Compactando arquivo final...")
    zip_path = zip_result(csv_path)
    print(f"   ✔ Arquivo ZIP gerado em: {zip_path}\n")

    print("✅ Pipeline finalizado com sucesso!\n")


if __name__ == "__main__":
    main()
