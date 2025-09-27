# src/etl_sap/pipelines/actividades.py
from __future__ import annotations

from typing import Dict, Sequence
import pandas as pd

from etl_project.loaders import ExcelLoader
from etl_project.transforms import (
    clean_column_names,
    drop_columns,
    filter_value,
    concat_columns,
)


class ActividadesPipeline:
    """
    Orquesta la carga y transformación del dataset 'actividades'
    según reglas declaradas en settings.yaml.
    """

    def __init__(self, loader: ExcelLoader, cfg: Dict):
        self.loader = loader
        self.cfg = cfg
        self.ds = cfg["datasets"]["actividades"]

    def _validate_required(self, df: pd.DataFrame, required: Sequence[str]) -> None:
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Faltan columnas requeridas: {missing}")

    def load(self) -> pd.DataFrame:
        # Descubrir y leer recursivamente
        source = self.ds["source"]
        subdir = source["folder"]
        patterns = tuple(source.get("patterns", ["*.xlsx", "*.xlsm"]))
        header = source.get("header", self.cfg["excel"].get("header", 0))
        engine_name = source.get("engine", self.cfg["excel"].get("engine", "openpyxl"))
        df = self.loader.read_many_recursive(
            subdir,
            patterns=patterns,
            header=header,
            engine=engine_name,
        )
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        tr = self.ds.get("transforms", {})

        # 1) Limpieza de columnas
        if tr.get("clean_columns", True):
            df = clean_column_names(df)

        # 2) Filtros (si existieran en YAML)
        for rule in tr.get("filters", []):
            df = filter_value(df, rule["column"], rule["value"], rule.get("op", "equals"))

        # 3) Eliminar columnas innecesarias
        cols_to_drop = tr.get("drop_columns", [])
        if cols_to_drop:
            df = drop_columns(df, cols_to_drop)

        # 4) Renombrados (si aplica)
        rename_map = tr.get("rename", {})
        if rename_map:
            df = df.rename(columns=rename_map)

        # 5) Nueva columna: id = fazenda_lote_talhao en la primera posición
        cc = tr.get("derive", {}).get("concat_columns")
        if cc:
            df = concat_columns(
                df,
                new_column=cc["new_column"],
                columns=cc["columns"],
                sep=cc.get("sep", "_"),
                position=cc.get("position", 0),
            )

        return df

    def run(self) -> pd.DataFrame:
        df = self.load()
        df = self.transform(df)
        return df