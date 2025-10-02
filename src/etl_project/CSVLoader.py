# csv_loader.py
import pandas as pd
import unicodedata
from .conexiondb import DatabaseConnection

def normalize_column_name(name: str) -> str:
    """Quita tildes, pasa a minúsculas, reemplaza espacios y puntos por guiones bajos"""
    nfkd = unicodedata.normalize("NFKD", name)
    no_accents = "".join([c for c in nfkd if not unicodedata.combining(c)])
    return (
        no_accents.strip()
        .lower()
        .replace(" ", "_")
        .replace(".", "") 
    )

class CSVLoader:
    def __init__(self, table_name: str, schema: str = "raw"):
        self.db = DatabaseConnection()
        self.table_name = table_name
        self.schema = schema
 
    def load_csv(self, csv_path: str, if_exists: str = "append"):
        """
        Carga un CSV a la tabla destino en PostgreSQL.
        """
        # Leer CSV
        df = pd.read_csv(csv_path)

        # Normalizar nombres de columnas
        df.columns = [normalize_column_name(col) for col in df.columns]
 
        # Conexión a la base
        engine = self.db.get_engine()

        # Limpiar la tabla si es necesario
        if if_exists == "replace":
            with engine.begin() as conn:  # begin = autocommit
                conn.execute(f"DELETE FROM {self.schema}.{self.table_name};")

        # Cargar en la tabla
        df.to_sql(
            self.table_name,
            engine,
            schema=self.schema,
            if_exists="append",  # usamos append siempre, ya borramos si era "replace"
            index=False
        )

        print(f"✅ {len(df)} filas cargadas en {self.schema}.{self.table_name}")