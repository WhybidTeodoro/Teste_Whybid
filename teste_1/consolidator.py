import csv
import zipfile
from pathlib import Path
from typing import List, Dict


OUTPUT_DIR = Path("output")
CSV_FILENAME = "despesas_eventos_sinistros.csv"
ZIP_FILENAME = "consolidado_despesas.zip"


def ensure_output_dir() -> None:
    """
    Garante que o diretório de saída exista.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def write_csv(data: List[Dict[str, object]]) -> Path:
    """
    Gera o arquivo CSV consolidado com os dados filtrados.
    """
    ensure_output_dir()

    csv_path = OUTPUT_DIR / CSV_FILENAME

    fieldnames = [
        "CNPJ",
        "RazaoSocial",
        "Ano",
        "Trimestre",
        "ValorDespesas"
    ]

    with csv_path.open(mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=fieldnames,
            delimiter=";"
        )
        writer.writeheader()
        writer.writerows(data)

    return csv_path


def zip_result(csv_path: Path) -> Path:
    """
    Compacta o CSV final em um arquivo ZIP.
    """
    zip_path = OUTPUT_DIR / ZIP_FILENAME

    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(
            filename=csv_path,
            arcname=csv_path.name
        )

    return zip_path
