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

        # Variables de la base de datos
        db = self.config.get("database", {})
        self.DB_HOST = os.getenv("DB_HOST", db.get("HOST")) or _get_secret("DB_HOST")
        self.DB_PORT = os.getenv("DB_PORT", db.get("PORT")) or _get_secret("DB_PORT", "5432")
        self.DB_NAME = os.getenv("DB_NAME", db.get("DB_NAME")) or _get_secret("DB_NAME")
        self.DB_USER = os.getenv("DB_USER", db.get("USER")) or _get_secret("DB_USER")
        self.DB_PASSWORD = os.getenv("DB_PASSW", db.get("PASSWORD")) or _get_secret("DB_PASSW")

        # Rutas de archivos
        paths = self.config.get("paths", {})
        self.DATA_PATH = paths.get("DATA_PATH")

    def get_db_uri(self) -> str:
        """Devuelve la URI estilo SQLAlchemy."""
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )
