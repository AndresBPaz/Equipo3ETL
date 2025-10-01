import yaml
from pathlib import Path

class Config:
    def __init__(self, yaml_file="config/settings.yaml"):
        # Construye la ruta absoluta al archivo settings.yaml
        self.yaml_file_path = Path(__file__).parent.parent.parent / yaml_file
        with open(self.yaml_file_path, "r") as f:
            self.config = yaml.safe_load(f)

        # Variables de la base de datos
        db = self.config.get("database", {})
        self.DB_HOST = db.get("HOST")
        self.DB_PORT = db.get("PORT")
        self.DB_NAME = db.get("DB_NAME")
        self.DB_USER = db.get("USER")
        self.DB_PASSWORD = db.get("PASSWORD")

        # Rutas de archivos
        self.DATA_PATH = db.get("DATA_PATH")



    def get_db_uri(self):
        """Devuelve la URI estilo SQLAlchemy"""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"