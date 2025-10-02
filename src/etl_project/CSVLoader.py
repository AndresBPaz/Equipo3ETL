# etl_project/CSVLoader.py
import pandas as pd
import unicodedata
from .conexiondb import DatabaseConnection

def normalize_column_name(name: str) -> str:
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
        df = pd.read_csv(csv_path)
        df.columns = [normalize_column_name(col) for col in df.columns]

        engine = self.db.get_engine()

        # Log seguro de destino (oculta password por defecto en SQLAlchemy 2.x)
        try:
            url = engine.url  # URL object
            # Construir cadena sin contraseña de forma explícita
            safe_url = url.render_as_string(hide_password=True)  # recomendado
            print(f"[CSVLoader] Destino: {safe_url}")
            # O bien manual:
            # print(f"[CSVLoader] Destino: postgresql://{url.username}@{url.host}:{url.port}/{url.database}")
        except Exception:
            pass  # no bloquear si falla el log

        if if_exists == "replace":
            with engine.begin() as conn:
                conn.execute(f"DELETE FROM {self.schema}.{self.table_name};")

        df.to_sql(
            self.table_name,
            engine,
            schema=self.schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000,
        )
        print(f"✅ {len(df)} filas cargadas en {self.schema}.{self.table_name}")
