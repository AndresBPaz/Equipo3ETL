from __future__ import annotations

from typing import Dict
import pandas as pd

from etl_project.loaders import ExcelLoader
from etl_project.transforms import (
    clean_column_names,
    delete_columns,
    concat_columns,
    adjust_date_format,
    concat_column_with_first_n,
)

class InsumosPipeline:
    """
    Orquesta la carga y transformación del dataset 'insumos'
    según reglas declaradas en settings.yaml.
    """

    def __init__(self, loader: ExcelLoader, cfg: Dict):
        self.loader = loader
        self.cfg = cfg
        self.ds = cfg["datasets"]["insumos"]
        
    def extract(self) -> pd.DataFrame:
        """
        Descubre archivos recursivamente y concatena en un único DataFrame.
        """
        source = self.ds["source"]
        subdir = source["folder"]
        patterns = tuple(source.get("patterns", ["*.xlsx", "*.xlsm"]))
        header = source.get("header", self.cfg["excel"].get("header", 0))
        engine_name = source.get("engine", self.cfg["excel"].get("engine", "openpyxl"))
        
        # Sirve para leer multiples excels en una carpeta
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

        # 3. Eliminar columnas innecesarias
        drops = tr.get("drop_columns", [])
        if drops:
            df = delete_columns(df, drops)

        # 3) Renombrar columnas
        rename_map = tr.get("rename", {})
        if rename_map:
            df = df.rename(columns=rename_map)

        # 4. Ajustar formato de fechas según YAML
        date_adjust = tr.get("derive", {}).get("adjust_date_format")
        if date_adjust:
            df = adjust_date_format(
                df=df,
                column_name=date_adjust.get("column", "fecha"),
                current_format=date_adjust["current_format"],
                desired_format=date_adjust["desired_format"],
            )

        # 5 Crear mieva columna: hda = primeros n caracteres de nm_faz (posicional si se indica)
        firstn = tr.get("derive", {}).get("concat_column_with_first_n")
        if firstn:
            df = concat_column_with_first_n(
                df=df,
                new_column=firstn["new_column"],
                column_name=firstn["column"],
                n=firstn["n"],
                position=firstn.get("position", 0),
            )

        # 6. Nueva columna: id = hda_lote_tal en la primera posición
        cc = tr.get("derive", {}).get("concat_columns")
        if cc:
            df = concat_columns(
                df=df,
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
