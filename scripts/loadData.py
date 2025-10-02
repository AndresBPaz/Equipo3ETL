import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from etl_project.CSVLoader import CSVLoader
from etl_project.config import Config
from etl_project.conexiondb import DatabaseConnection  # importa tu clase de conexión

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

    def _log_connection(self):
        db = DatabaseConnection()
        engine = db.get_engine()
        try:
            url = engine.url
            safe_url = url.render_as_string(hide_password=True)
            print(f"[loadData] Conectando a: {safe_url}")
        except Exception:
            pass
        finally:
            db.close()

    def run(self):
        self._log_connection()
        for job in self.jobs:
            print(f"[loadData] Cargando {job['table']}...")
            loader = CSVLoader(table_name=job["table"], schema=job["schema"])
            loader.load_csv(job["file"], if_exists="append")
            print(f"[loadData] OK -> {job['table']}")

if __name__ == "__main__":
    pipeline = LoadData()
    pipeline.run()