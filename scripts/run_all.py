from pathlib import Path
import yaml

from etl_project.loaders import ExcelLoader
from etl_project.pipelines.abastecimientos import AbastecimientosPipeline
from etl_project.pipelines.actividades import ActividadesPipeline
from etl_project.pipelines.insumos import InsumosPipeline
from etl_project.pipelines.rep_maquinaria import RepMaquinariaPipeline

def load_settings(path="config/settings.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def run():
    cfg = load_settings("config/settings.yaml")

    base = Path(cfg["paths"]["base"])
    processed_dir = base / cfg["paths"]["data_processed"]
    processed_dir.mkdir(parents=True, exist_ok=True)

    loader = ExcelLoader(base)

    jobs = [
        ("abastecimientos", AbastecimientosPipeline, "abastecimientos"),
        ("actividades",     ActividadesPipeline,     "actividades"),
        ("insumos",         InsumosPipeline,         "insumos"),
        ("rep_maquinaria",  RepMaquinariaPipeline,   "rep_maquinaria"),
    ]

    for ds_name, Pipeline, stem in jobs:
        print(f"[run_all] Ejecutando {ds_name}...")
        pipeline = Pipeline(loader, cfg)
        df = pipeline.run()

        # Parquet
        pq_path = processed_dir / f"{stem}.parquet"
        df.to_parquet(pq_path, index=False)  # requiere pyarrow o fastparquet
        # CSV
        csv_path = processed_dir / f"{stem}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8", date_format="%d/%m/%Y")

        print(f"[run_all] OK -> {pq_path.name} y {csv_path.name}")

if __name__ == "__main__":
    run()
