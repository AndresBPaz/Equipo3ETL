# db_connection.py
from sqlalchemy import create_engine, URL
from sqlalchemy.engine import Engine
from .config import Config

class DatabaseConnection:
    def __init__(self):
        self.config = Config("config/settings.yaml")
        self.engine: Engine = None

    def connect(self):
        """Crea una conexión usando SQLAlchemy"""
        if self.engine is None:
            # Construir la URL de conexión usando sqlalchemy.URL
            db_url = URL.create(
                drivername="postgresql+psycopg2", # Especifica el driver psycopg2
                username=self.config.DB_USER,
                password=self.config.DB_PASSWORD,
                host=self.config.DB_HOST,
                port=self.config.DB_PORT,
                database=self.config.DB_NAME,
            )
            self.engine = create_engine(db_url)
        return self.engine

    def get_engine(self):
        """Devuelve el engine ya creado"""
        if not self.engine:
            return self.connect()
        return self.engine

    def close(self):
        """Cierra la conexión"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
    
    def query(self, sql_query):
        """Ejecuta una consulta SQL y devuelve los resultados"""
        if not self.engine:
            self.connect()
        with self.engine.connect() as connection:
            result = connection.execute(sql_query)
            return result.fetchall()

# Ejemplo de uso
""" if __name__ == "__main__":
    db = DatabaseConnection()
    engine = db.get_engine()
    print("Conexión exitosa:", engine)
    tbl_abastecimientos = db.query("SELECT * FROM abastecimientos LIMIT 5;")
    print(tbl_abastecimientos)
    db.close()
    print("Conexión cerrada") 
     """