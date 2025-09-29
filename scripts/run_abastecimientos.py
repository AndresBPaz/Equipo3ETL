from pathlib import Path
import yaml

from etl_project.loaders import ExcelLoader
from etl_project.pipelines.abastecimientos import AbastecimientosPipeline

def load_settings(path="config/settings.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def run():
    cfg = load_settings("config/settings.yaml")

    # 1) Preparar loader y pipeline
    base = Path(cfg["paths"]["base"])
    loader = ExcelLoader(base)
    pipeline = AbastecimientosPipeline(loader, cfg)

    # 2) Ejecutar: load -> transform (según YAML)
    df = pipeline.run()

    # 3) Guardar a processed como Parquet
    processed_dir = base / cfg["paths"]["data_processed"]
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_path = processed_dir / "abastecimientos.parquet"
    df.to_parquet(out_path, index=False) 

if __name__ == "__main__":
    run()