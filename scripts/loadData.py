import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from etl_project.CSVLoader import CSVLoader
from etl_project.config import Config

class LoadData:
    def __init__(self):
        self.cfg = Config("config/settings.yaml")
        self.jobs = [
            {
                "table": "abastecimientos",
                "schema": "raw",
                "file": Path(__file__).parent.parent / Path(self.cfg.DATA_PATH) / "processed/abastecimientos.csv"
            },
            {
                "table": "actividades",
                "schema": "raw",
                "file": Path(__file__).parent.parent / Path(self.cfg.DATA_PATH) / "processed/actividades.csv"
            },
            {
                "table": "insumos",
                "schema": "raw",
                "file": Path(__file__).parent.parent / Path(self.cfg.DATA_PATH) / "processed/insumos.csv"
            },
            {
                "table": "rep_maquinaria",
                "schema": "raw",
                "file": Path(__file__).parent.parent / Path(self.cfg.DATA_PATH) / "processed/rep_maquinaria.csv"
            }, 
            # aquí agregas más datasets:
            # {"table": "actividades", "schema": "raw", "file": Path(self.cfg.DATA_PATH) / "actividades.csv"},
            # {"table": "insumos", "schema": "raw", "file": Path(self.cfg.DATA_PATH) / "insumos.csv"},
        ]

    def run(self):
        for job in self.jobs:
            print(f"[loadData] Cargando {job['table']}...")
            # etl_project/CSVLoader.py (antes de df.to_sql)
            print(f"[loadData] Conectando a {loader.db.config.DB_HOST}:{loader.db.config.DB_PORT}/{loader.db.config.DB_NAME} como {loader.db.config.DB_USER}")
            loader = CSVLoader(table_name=job["table"], schema=job["schema"])
            loader.load_csv(job["file"], if_exists="append")
            print(f"[loadData] OK -> {job['table']}")
            
if __name__ == "__main__":
    pipeline = LoadData()
    pipeline.run()