from __future__ import annotations

from typing import Dict, Sequence
import pandas as pd

from etl_project.loaders import ExcelLoader
from etl_project.transforms import (
    clean_column_names,
    drop_columns,
    adjust_date_format,
    concat_columns,
)

class RepMaquinariaPipeline:
    """
    Orquesta la carga y transformación del dataset 'rep_maquinaria'
    según reglas declaradas en settings.yaml.
    """

    def __init__(self, loader: ExcelLoader, cfg: Dict):
        self.loader = loader
        self.cfg = cfg
        self.ds = cfg["datasets"]["rep_maquinaria"]

    def _validate_required(self, df: pd.DataFrame, required: Sequence[str]) -> None:
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Faltan columnas requeridas: {missing}")

    def load(self) -> pd.DataFrame:
        src = self.ds["source"]
        df = self.loader.read_many_recursive(
            src["folder"],
            patterns=tuple(src.get("patterns", ["*.xlsx", "*.xlsm"])),
            header=src.get("header", self.cfg["excel"].get("header", 0)),
            engine=src.get("engine", self.cfg["excel"].get("engine", "openpyxl")),
        )
        return df  # pandas.read_excel respeta engine/header y soporta .xlsx/.xlsm [web:30].

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        tr = self.ds.get("transforms", {})

        # 1) Limpieza de nombres de columnas
        if tr.get("clean_columns", True):
            df = clean_column_names(df)  # Normaliza nombres para facilitar drops/renames [web:30].

        # 2) Eliminar columnas innecesarias
        drops = tr.get("drop_columns", [])
        if drops:
            df = drop_columns(df, drops)  # drop con errors='ignore' evita fallos por columnas faltantes [web:312].

        # 3) Renombrados (ej.: equip->equipo, hacienda_o.s->hda, etc.)
        ren = tr.get("rename", {})
        if ren:
            df = df.rename(columns=ren)  # DataFrame.rename con mapeo dict de columnas [web:312].

        # 4) Ajuste de formato de fecha (ej.: "%d/%m/%Y %I:%M:%S %p" -> "%d/%m/%Y")
        adf = tr.get("derive", {}).get("adjust_date_format")
        if adf:
            df = adjust_date_format(
                df=df,
                column_name=adf.get("column", "fecha"),
                current_format=adf["current_format"],
                desired_format=adf["desired_format"],
            )  # to_datetime con format + dt.strftime produce strings con el patrón deseado [web:428][web:439].

        # 5) id = hda_lote_tal (posición según YAML)
        cc = tr.get("derive", {}).get("concat_columns")
        if cc:
            df = concat_columns(
                df=df,
                new_column=cc["new_column"],
                columns=cc["columns"],
                sep=cc.get("sep", "_"),
                position=cc.get("position", 0),
            )  # Concatena columnas como texto con separador y ubica la nueva en la posición indicada [web:439].

        return df

    def validate(self, df: pd.DataFrame) -> None:
        required = self.ds.get("validate", {}).get("required_columns", [])
        if required:
            self._validate_required(df, required)

    def run(self) -> pd.DataFrame:
        df = self.load()
        df = self.transform(df)
        self.validate(df)
        return df