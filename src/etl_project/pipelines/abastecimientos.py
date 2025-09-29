from __future__ import annotations

from typing import Dict
import pandas as pd

from etl_project.loaders import ExcelLoader
from etl_project.transforms import (
    clean_column_names,
    delete_columns,
    filter_value,
    delete_first_n,
)

class AbastecimientosPipeline:
    """
    Orquesta la carga y transformación del dataset 'abastecimientos'
    según reglas definidas en settings.yaml (source, transforms, validate).
    """

    def __init__(self, loader: ExcelLoader, cfg: Dict):
        self.loader = loader
        self.cfg = cfg
        self.ds = cfg["datasets"]["abastecimientos"]

    def extract(self) -> pd.DataFrame:
        """
        Descubre archivos recursivamente y concatena en un único DataFrame.
        """
        source = self.ds["source"]
        subdir = source["folder"]
        patterns = tuple(source.get("patterns", ["*.xlsx", "*.xlsm"]))
        header = source.get("header", self.cfg["excel"].get("header", 0))
        engine_name = source.get("engine", self.cfg["excel"].get("engine", "openpyxl"))

        #Sirve para leer multiples excels en una carpeta
        df = self.loader.read_many_recursive(
            subdir,
            patterns=patterns,
            header=header,
            engine=engine_name,
        )
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica limpieza de columnas, filtros, renombres y derivaciones.
        """
        tr = self.ds.get("transforms", {})

        # 1. Limpieza de nombres de columnas
        if tr.get("clean_columns", True):
            df = clean_column_names(df)

        # 2. Filtros (usar nombres mas claros)
        for rule in tr.get("filters", []):
            df = filter_value(df, rule["column"], rule["value"], rule.get("op", "equals"))

        # 3. Eliminar columnas innecesarias
        cols_to_drop = tr.get("drop_columns", [])
        if cols_to_drop:
            df = delete_columns(df, cols_to_drop)

        # 4. Renombrar columnas
        rename_map = tr.get("rename", {})
        if rename_map:
            df = df.rename(columns=rename_map)

        # 5. Se llama a delete_first_n que elimina los primeros n caracteres de los valores de una columna
        derive = tr.get("derive", {})
        if "delete_first_n" in derive:
            df = delete_first_n(
                df,
                derive["delete_first_n"]["column"],
                derive["delete_first_n"]["n"],
            )

        return df

    def run(self) -> pd.DataFrame:
        """
        Ejecuta Extract -> transform y retorna el DataFrame final.
        """
        df = self.extract()
        df = self.transform(df)
        return df