from pathlib import Path
import yaml
import pandas as pd


from etl_project.transforms import (
    clean_column_names,
    drop_columns,
    filter_value,
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
    ds = cfg["datasets"]["actividades"]

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
    tr = ds.get("transforms", {})

    if tr.get("clean_columns", True):
        df = clean_column_names(df)

    for rule in tr.get("filters", []):
        df = filter_value(df, rule["column"], rule["value"], rule.get("op", "equals"))

    cols_to_drop = tr.get("drop_columns", [])
    if cols_to_drop:
        df = drop_columns(df, cols_to_drop)

    rename_map = tr.get("rename", {})
    if rename_map:
        df = df.rename(columns=rename_map)

    # Nuevo atributo id: concatenar columnas (ej.: id = fazenda_lote_talhao)
    cc = tr.get("derive", {}).get("concat_columns")
    if cc:
        df = concat_columns(
            df,
            new_column=cc["new_column"],
            columns=cc["columns"],
            sep=cc.get("sep", "_"),
            position=cc.get("position", 0),
        )

    # 3) Guardar a processed como Parquet
    processed_dir = Path(cfg["paths"]["data_processed"])
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_path = processed_dir / "actividades.parquet"
    df.to_parquet(out_path, index=False)

if __name__ == "__main__":
    run()
