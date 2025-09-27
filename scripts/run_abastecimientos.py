# scripts/run_abastecimientos.py
from pathlib import Path
import yaml
import pandas as pd

# Importa utilidades de transformación que tengas en src/etl_sap/transforms.py
from etl_project.transforms import (
    clean_column_names,
    drop_columns,
    filter_value,
    delete_first_n,
)

def load_settings(path="config/settings.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def list_files_recursive(folder, patterns=("*.xlsx","*.xlsm")):
    base = Path(folder)
    files = []
    for pat in patterns:
        files.extend(base.rglob(pat))
    return files

def run():
    cfg = load_settings("config/settings.yaml")
    ds = cfg["datasets"]["abastecimientos"]

    # 1) Descubrir y leer archivos
    folder = ds["source"]["folder"]
    patterns = tuple(ds["source"].get("patterns", ["*.xlsx", "*.xlsm"]))
    header = ds["source"].get("header", cfg["excel"].get("header", 0))
    engine_name = ds["source"].get("engine", cfg["excel"].get("engine", "openpyxl"))

    files = list_files_recursive(folder, patterns=patterns)
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos en {folder} con {patterns}")

    dfl = [pd.read_excel(p, engine=engine_name, header=header) for p in files]
    df = pd.concat(dfl, ignore_index=True)

    # 2) Transformaciones declaradas en settings.yaml
    if ds["transforms"].get("clean_columns", True):
        df = clean_column_names(df)

    for rule in ds["transforms"].get("filters", []):
        df = filter_value(df, rule["column"], rule["value"], rule.get("op", "equals"))
        
    cols_to_drop = ds["transforms"].get("drop_columns", [])
    if cols_to_drop:
        df = drop_columns(df, cols_to_drop)

    rename_map = ds["transforms"].get("rename", {})
    if rename_map:
        df = df.rename(columns=rename_map)

    derive = ds["transforms"].get("derive", {})
    if "delete_first_n" in derive:
        df = delete_first_n(df, derive["delete_first_n"]["column"], derive["delete_first_n"]["n"])

    # 3) Validación simple
    required = ds.get("validate", {}).get("required_columns", [])
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    # 4) Guardar a processed en Parquet
    processed_dir = Path(cfg["paths"]["data_processed"])
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_path = processed_dir / "abastecimientos.parquet"
    df.to_parquet(out_path, index=False)  # requiere pyarrow o fastparquet

if __name__ == "__main__":
    run()
