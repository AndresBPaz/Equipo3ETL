from __future__ import annotations

from typing import Dict
import pandas as pd

from etl_project.loaders import ExcelLoader
from etl_project.transforms import (
    clean_column_names,
    delete_columns,
    filter_value,
    concat_columns,
    adjust_date_format,
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

        # 2. Filtros (en este caso no hay, pero se deja la estructura, por si se agrega en el settings.yaml)
        for rule in tr.get("filters", []):
            df = filter_value(df, rule["column"], rule["value"], rule.get("op", "equals"))

        # 3. Eliminar columnas innecesarias
        cols_to_drop = tr.get("drop_columns", [])
        if cols_to_drop:
            df = delete_columns(df, cols_to_drop)

        # 4) Renombrar columnas
        rename_map = tr.get("rename", {})
        if rename_map:
            df = df.rename(columns=rename_map)
            
        # 5. Ajustar formato de fecha
        adf = tr.get("adjust_date_format", {})
        if adf:
            df = adjust_date_format(
                df,
                column_name=adf["column"],
                current_format=adf["current_format"],
                desired_format=adf["desired_format"],
            )

        # 6. Nueva columna: id = fazenda_lote_talhao en la primera posición
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
        """
        Ejecuta Extract -> transform y retorna el DataFrame final.
        """
        df = self.extract()
        df = self.transform(df)
        return df