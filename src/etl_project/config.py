"""Configuration helpers for database and file paths."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import yaml

try:  # Streamlit no siempre está disponible
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover - entornos sin streamlit
    st = None  # type: ignore


def _get_secret(key: str, default: Optional[Any] = None) -> Optional[Any]:
    """Safely fetch a Streamlit secret without exploding outside Streamlit."""
    if st is None:
        return default
    try:
        return st.secrets.get(key, default)  # type: ignore[attr-defined]
    except Exception:
        return default


class Config:
    def __init__(self, yaml_file: str = "config/settings.yaml") -> None:
        # Construye la ruta absoluta al archivo settings.yaml
        self.yaml_file_path = Path(__file__).parent.parent.parent / yaml_file
        with open(self.yaml_file_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        # Variables de la base de datos - prioridad: ENV > secrets > YAML
        db = self.config.get("database", {})
        self.DB_HOST = os.getenv("DB_HOST") or _get_secret("DB_HOST") or db.get("HOST")
        self.DB_PORT = os.getenv("DB_PORT") or _get_secret("DB_PORT") or db.get("PORT", "5432")
        self.DB_NAME = os.getenv("DB_NAME") or _get_secret("DB_NAME") or db.get("DB_NAME")
        self.DB_USER = os.getenv("DB_USER") or _get_secret("DB_USER") or db.get("USER")
        self.DB_PASSWORD = os.getenv("DB_PASSW") or _get_secret("DB_PASSW") or db.get("PASSWORD")

        # Validar que todas las credenciales estén presentes
        self._validate_db_config()

        # Rutas de archivos
        paths = self.config.get("paths", {})
        self.DATA_PATH = paths.get("DATA_PATH")

    def _validate_db_config(self) -> None:
        """Valida que todas las variables de DB estén configuradas."""
        missing = []
        if not self.DB_HOST:
            missing.append("DB_HOST")
        if not self.DB_PORT:
            missing.append("DB_PORT")
        if not self.DB_NAME:
            missing.append("DB_NAME")
        if not self.DB_USER:
            missing.append("DB_USER")
        if not self.DB_PASSWORD:
            missing.append("DB_PASSWORD")
        
        if missing:
            raise ValueError(
                f"❌ Faltan las siguientes variables de DB: {', '.join(missing)}\n"
                f"   Configúralas en environment variables, secrets o settings.yaml"
            )
        
        # Log de configuración (sin password)
        print(f"[Config] DB: {self.DB_USER}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}")

    def get_db_uri(self) -> str:
        """Devuelve la URI estilo SQLAlchemy."""
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@"
            f"{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )
