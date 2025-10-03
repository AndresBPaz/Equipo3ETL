# csv_loader.py
import pandas as pd
import unicodedata
from sqlalchemy import text
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
        print(f"[CSVLoader] Procesando {csv_path}...")
        
        # Leer CSV
        df = pd.read_csv(csv_path)
        print(f"[CSVLoader] {len(df)} filas leídas del CSV")

        # Normalizar nombres de columnas
        df.columns = [normalize_column_name(col) for col in df.columns]
 
        # Conexión a la base
        engine = self.db.get_engine()
        
        if engine is None:
            raise RuntimeError("❌ No se pudo establecer conexión con la base de datos")

        # Asegurar que el esquema existe - IMPORTANTE: usar text()
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))
        
        print(f"[CSVLoader] Esquema '{self.schema}' verificado")

        # Limpiar la tabla si es necesario - IMPORTANTE: usar text()
        if if_exists == "replace":
            with engine.begin() as conn:
                conn.execute(
                    text(f"DELETE FROM {self.schema}.{self.table_name}")
                )
            print(f"[CSVLoader] Tabla {self.schema}.{self.table_name} limpiada")

        # Cargar en la tabla
        df.to_sql(
            self.table_name,
            engine,
            schema=self.schema,
            if_exists="append",
            index=False,
            method="multi",  # Inserciones por lotes para mejor rendimiento
            chunksize=1000
        )

        print(f"✅ {len(df)} filas cargadas en {self.schema}.{self.table_name}")
