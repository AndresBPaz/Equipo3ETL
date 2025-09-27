# scripts/run_insumos.py
from pathlib import Path
import yaml
import pandas as pd

# Importa utilidades de transformación
from etl_project.transforms import (
    clean_column_names,
    drop_columns,
    adjust_date_format,
    concat_column_with_first_n,
    concat_columns,
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
    ds = cfg["datasets"]["insumos"]

    # 1) Descubrir y leer archivos
    folder = ds["source"]["folder"]
    patterns = tuple(ds["source"].get("patterns", ["*.xlsx", "*.xlsm"]))
    header = ds["source"].get("header", cfg["excel"].get("header", 0))
    engine_name = ds["source"].get("engine", cfg["excel"].get("engine", "openpyxl"))

    files = list_files_recursive(folder, patterns=patterns)
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos en {folder} con {patterns}")

    # Leer todos los Excel y concatenar
    dfl = [pd.read_excel(p, engine=engine_name, header=header) for p in files]
    df = pd.concat(dfl, ignore_index=True)

    # 2) Transformaciones según settings.yaml
    tr = ds.get("transforms", {})

    if tr.get("clean_columns", True):
        df = clean_column_names(df)

    drops = tr.get("drop_columns", [])
    if drops:
        df = drop_columns(df, drops)

    rename_map = tr.get("rename", {})
    if rename_map:
        df = df.rename(columns=rename_map)

    # Ajuste de formato de fecha
    adj = tr.get("derive", {}).get("adjust_date_format")
    if adj:
        df = adjust_date_format(
            df=df,
            column_name=adj.get("column", "fecha"),
            current_format=adj["current_format"],
            desired_format=adj["desired_format"],
        )

    # hda = primeros n caracteres de nm_faz
    firstn = tr.get("derive", {}).get("concat_column_with_first_n")
    if firstn:
        df = concat_column_with_first_n(
            df=df,
            new_column=firstn["new_column"],
            column_name=firstn["column"],
            n=firstn["n"],
            position=firstn.get("position", 0),
        )

    # id = hda_lote_tal
    cc = tr.get("derive", {}).get("concat_columns")
    if cc:
        df = concat_columns(
            df=df,
            new_column=cc["new_column"],
            columns=cc["columns"],
            sep=cc.get("sep", "_"),
            position=cc.get("position", 0),
        )

    # 3) Validación opcional
    required = ds.get("validate", {}).get("required_columns", [])
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    # 4) Guardar a processed como Parquet
    processed_dir = Path(cfg["paths"]["data_processed"])
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_path = processed_dir / "insumos.parquet"
    df.to_parquet(out_path, index=False)  # requiere pyarrow o fastparquet

if __name__ == "__main__":
    run()
