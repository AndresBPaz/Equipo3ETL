# scripts/run_actividades.py
from pathlib import Path
import yaml

from etl_project.loaders import ExcelLoader
from etl_project.pipelines.actividades import ActividadesPipeline

def load_settings(path="config/settings.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def run():
    cfg = load_settings("config/settings.yaml")

    # 1) Preparar loader y pipeline
    base = Path(cfg["paths"]["base"])
    loader = ExcelLoader(base)
    pipeline = ActividadesPipeline(loader, cfg)

    # 2) Ejecutar: Extract -> transform -> validate (si aplica)
    df = pipeline.run()

    # 3) Guardar a processed como Parquet
    processed_dir = base / cfg["paths"]["data_processed"]
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_path = processed_dir / "actividades.parquet"
    df.to_parquet(out_path, index=False)

if __name__ == "__main__":
    run()
