from __future__ import annotations
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple, Union, Dict
import pandas as pd

class ExcelLoader:
    """
    Loader de archivos Excel con descubrimiento recursivo y lectura parametrizable.
    """
    
    def __init__(self, base:Union[str, Path] = "."):
        self.base = Path(base)
        
    def read_one(
        self,
        path: Union[str, Path],
        *,
        header: int = 0,
        engine: str = "openpyxl",
        sheet_name: Union[str, int, List[Union[str, int]], None] = 0,
        dtype: Optional[Union[str, Dict[str, str]]] = None,
        usecols: Optional[Union[str, List[str]]] = None,
        skiprows: Optional[Union[int, List[int]]] = None,
    ) -> pd.DataFrame:
        """
        Lee un archivo Excel a DataFrame con pandas.read_excel.
        """
        path = Path(path)
        df = pd.read_excel(
            path,
            header=header,
            engine=engine,
            sheet_name=sheet_name,
            dtype=dtype,
            usecols=usecols,
            skiprows=skiprows,
        )
        
        if isinstance(df, dict):
            parts = []
            for sh, dfi in df.items():
                dfi["sheet"] = sh
                parts.append(dfi)
            return pd.concat(parts, ignore_index=True)
        return df
    
    def read_all_sheets(
        self,
        path: Union[str, Path],
        *,
        header: int = 0,
        engine: str = "openpyxl",
    ) -> pd.DataFrame:
        """
        Lee todas las hojas de un Excel (sheet_name=None) y concatena con columna 'sheet'.
        """
        return self.read_one(path, header=header, engine=engine, sheet_name=None)
    
    def read_many_recursive(
        self,
        subdir: Union[str, Path],
        *,
        patterns: Sequence[str] = ("*.xlsx", "*.xlsm"),
        header: int = 0,
        engine: str = "openpyxl",
        sheet_name: Union[str, int, List[Union[str, int]], None] = 0,
        dtype: Optional[Union[str, Dict[str, str]]] = None,
        usecols: Optional[Union[str, List[str]]] = None,
        skiprows: Optional[Union[int, List[int]]] = None,
    ) -> pd.DataFrame:
        """
        Busca recursivamente archivos que coincidan con 'patterns' y los lee en un solo DataFrame.
        """
        root = self.base / subdir
        files: List[Path] = []
        for pat in patterns:
            files.extend(root.rglob(pat))
        if not files:
            raise FileNotFoundError(f"No se encontraron archivos en {root} con {patterns}")
        return self.read_many(
            files,
            header=header,
            engine=engine,
            sheet_name=sheet_name,
            dtype=dtype,
            usecols=usecols,
            skiprows=skiprows,
        )