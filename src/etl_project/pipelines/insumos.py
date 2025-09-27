from __future__ import annotations

from typing import Dict, Sequence
import pandas as pd

from etl_project.loaders import ExcelLoader
from etl_project.transforms import (
    clean_column_names,
    drop_columns,
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

    def _validate_required(self, df: pd.DataFrame, required: Sequence[str]) -> None:
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Faltan columnas requeridas: {missing}")

    def load(self) -> pd.DataFrame:
        source = self.ds["source"]
        df = self.loader.read_many_recursive(
            source["folder"],
            patterns=tuple(source.get("patterns", ["*.xlsx", "*.xlsm"])),
            header=source.get("header", self.cfg["excel"].get("header", 0)),
            engine=source.get("engine", self.cfg["excel"].get("engine", "openpyxl")),
        )
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        tr = self.ds.get("transforms", {})

        # 1) Limpieza de columnas
        if tr.get("clean_columns", True):
            df = clean_column_names(df)

        # 2) Eliminar columnas innecesarias
        drops = tr.get("drop_columns", [])
        if drops:
            df = drop_columns(df, drops)

        # 3) Renombrar columnas (p. ej. data_apli -> fecha)
        rename_map = tr.get("rename", {})
        if rename_map:
            df = df.rename(columns=rename_map)

        # 4) Ajustar formato de fechas según YAML
        date_adjust = tr.get("derive", {}).get("adjust_date_format")
        if date_adjust:
            df = adjust_date_format(
                df=df,
                column_name=date_adjust.get("column", "fecha"),
                current_format=date_adjust["current_format"],
                desired_format=date_adjust["desired_format"],
            )
            # pd.to_datetime con 'format' y dt.strftime implementan el cambio de formato de forma robusta.

        # 5) hda = primeros n caracteres de nm_faz (posicional si se indica)
        firstn = tr.get("derive", {}).get("concat_column_with_first_n")
        if firstn:
            df = concat_column_with_first_n(
                df=df,
                new_column=firstn["new_column"],
                column_name=firstn["column"],
                n=firstn["n"],
                position=firstn.get("position", 0),
            )

        # 6) id = hda_lote_tal en la posición indicada
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
        df = self.load()
        df = self.transform(df)
        self.validate(df)
        return df
