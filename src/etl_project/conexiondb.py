# db_connection.py
from sqlalchemy import create_engine, URL
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from .config import Config
import time


class DatabaseConnection:
    def __init__(self):
        self.config = Config("config/settings.yaml")
        self.engine: Engine = None

    def connect(self, max_retries: int = 5):
        """Crea una conexión usando SQLAlchemy con reintentos."""
        delay = 2
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                if self.engine is None:
                    # Construir la URL de conexión usando sqlalchemy.URL
                    db_url = URL.create(
                        drivername="postgresql+psycopg2",
                        username=self.config.DB_USER,
                        password=self.config.DB_PASSWORD,
                        host=self.config.DB_HOST,
                        port=int(self.config.DB_PORT),
                        database=self.config.DB_NAME,
                    )
                    
                    # Crear engine con configuraciones robustas
                    self.engine = create_engine(
                        db_url,
                        pool_pre_ping=True,  # Verifica conexiones antes de usar
                        pool_size=5,
                        max_overflow=10,
                        connect_args={
                            "connect_timeout": 10,
                            "options": "-c statement_timeout=30000"
                        }
                    )
                    
                    # Test de conexión
                    with self.engine.connect() as conn:
                        conn.execute("SELECT 1")
                    
                    print(f"✓ Conexión exitosa a la base de datos")
                    return self.engine
                    
            except OperationalError as e:
                last_error = e
                print(f"[Intento {attempt}/{max_retries}] Error de conexión: {e}")
                self.engine = None
                
                if attempt < max_retries:
                    print(f"   Reintentando en {delay}s...")
                    time.sleep(delay)
                    delay *= 1.5  # Backoff exponencial
                else:
                    print(f"❌ Conexión fallida después de {max_retries} intentos")
                    
            except Exception as e:
                print(f"❌ Error inesperado al conectar: {e}")
                self.engine = None
                raise
        
        if last_error:
            raise last_error
        
        return self.engine

    def get_engine(self):
        """Devuelve el engine ya creado o crea uno nuevo."""
        if not self.engine:
            return self.connect()
        return self.engine

    def close(self):
        """Cierra la conexión."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            print("✓ Conexión cerrada")
    
    def query(self, sql_query):
        """Ejecuta una consulta SQL y devuelve los resultados."""
        if not self.engine:
            self.connect()
        with self.engine.connect() as connection:
            result = connection.execute(sql_query)
            return result.fetchall()
