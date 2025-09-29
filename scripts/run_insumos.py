# scripts/run_insumos.py
from pathlib import Path
import yaml

from etl_project.loaders import ExcelLoader
from etl_project.pipelines.insumos import InsumosPipeline

def load_settings(path="config/settings.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def run():
    cfg = load_settings("config/settings.yaml")

    # 1) Preparar loader y pipeline
    base = Path(cfg["paths"]["base"])
    loader = ExcelLoader(base)
    pipeline = InsumosPipeline(loader, cfg)

    # 2) Ejecutar: load -> transform -> validate
    df = pipeline.run()  # La pipeline usa pandas.read_excel internamente según el YAML [web:30].

    # 3) Guardar a processed como Parquet
    processed_dir = base / cfg["paths"]["data_processed"]
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_path = processed_dir / "insumos.parquet"
    df.to_parquet(out_path, index=False)

if __name__ == "__main__":
    run()
