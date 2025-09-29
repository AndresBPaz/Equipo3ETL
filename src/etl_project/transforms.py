from __future__ import annotations
from typing import Optional, Sequence
import pandas as pd

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia nombres de columnas: quita espacios extremos, pasa a minúsculas,
    cambia espacios por guiones bajos y elimina paréntesis.
    """
    out = df.copy()
    out.columns = (
        out.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
    )
    return out

def drop_columns(df: pd.DataFrame, columns_to_delete: Sequence[str]) -> pd.DataFrame:
    """
    Elimina columnas si existen; ignora las que no están presentes.
    """
    return df.drop(columns=list(columns_to_delete), errors="ignore")

def filter_value(df: pd.DataFrame, column_name: str, value, cmp: str = "equals",) -> pd.DataFrame:
    """
    Filtra filas según una comparación sobre una columna.
    cmp admite: equals, not_equals, greater_than, less_than, in, not_in, between.
    """
    if column_name not in df.columns:
        raise KeyError(f"Columna no encontrada: {column_name}")

    if cmp == "equals":
        mask = df[column_name] == value
    elif cmp == "not_equals":
        mask = df[column_name] != value
    elif cmp == "greater_than":
        mask = df[column_name] > value
    elif cmp == "less_than":
        mask = df[column_name] < value
    elif cmp == "in":
        mask = df[column_name].isin(value if isinstance(value, (list, tuple, set)) else [value])
    elif cmp == "not_in":
        mask = ~df[column_name].isin(value if isinstance(value, (list, tuple, set)) else [value])
    elif cmp == "between":
        if not (isinstance(value, (list, tuple)) and len(value) == 2):
            raise ValueError("Para 'between', value debe ser [min, max].")
        lo, hi = value
        mask = df[column_name].between(lo, hi)
    else:
        raise ValueError(
            "cmp inválido. Use 'equals', 'not_equals', 'greater_than', 'less_than', 'in', 'not_in', o 'between'."
        )
    return df[mask]

def delete_first_n(df: pd.DataFrame, column_name: str, n: int) -> pd.DataFrame:
    """
    Elimina los primeros n caracteres de una columna, convirtiéndola a str si es necesario.
    """
    if column_name not in df.columns:
        raise KeyError(f"Columna no encontrada: {column_name}")
    out = df.copy()
    out[column_name] = out[column_name].astype(str).str.slice(start=n)
    return out

def concat_columns(
    df: pd.DataFrame,
    new_column: str,
    columns: Sequence[str],
    sep: str = "_",
    position: Optional[int] = None,
) -> pd.DataFrame:
    """
    Concatena columnas como strings con separador y crea una nueva columna.
    Si 'position' se especifica, inserta la nueva columna en ese índice.
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(f"Columnas no encontradas para concatenar: {missing}")

    # Construir la serie concatenada de forma vectorizada
    s = df[columns[0]].astype(str)
    for c in columns[1:]:
        s = s.str.cat(df[c].astype(str), sep=sep)

    out = df.copy()
    if position is None or position >= len(out.columns):
        out[new_column] = s
    else:
        out.insert(position, new_column, s)  # inserta en posición específica
    return out

def adjust_date_format(
    df: pd.DataFrame,
    column_name: str,
    current_format: str,
    desired_format: str,
) -> pd.DataFrame:
    """
    Ajusta el formato de fecha de una columna.
    current_format y desired_format usan códigos de strftime/strptime.
    """
    if column_name not in df.columns:
        raise KeyError(f"Columna no encontrada: {column_name}")
    out = df.copy()
    out[column_name] = pd.to_datetime(out[column_name], format=current_format, errors="coerce")
    out[column_name] = out[column_name].dt.strftime(desired_format)
    return out

def concat_column_with_first_n(
    df: pd.DataFrame,
    new_column: str,
    column_name: str,
    n: int,
    position: Optional[int] = None,
) -> pd.DataFrame:
    """
    Crea una nueva columna concatenando los primeros n caracteres de otra columna.
    Si 'position' se especifica, inserta la nueva columna en ese índice.
    """
    if column_name not in df.columns:
        raise KeyError(f"Columna no encontrada: {column_name}")

    out = df.copy()
    out[new_column] = out[column_name].astype(str).str.slice(stop=n)

    if position is not None and position < len(out.columns):
        col_data = out.pop(new_column)
        out.insert(position, new_column, col_data)  # inserta en posición específica

    return out

# Aliases para las anteriores funciones con nombres más descriptivos
def delete_columns(df: pd.DataFrame, columns_to_delete: Sequence[str]) -> pd.DataFrame:
    return drop_columns(df, columns_to_delete)


def filter_rows_by_value(
    df: pd.DataFrame, column_name: str, value, comparison_type: str = "equals"
) -> pd.DataFrame:
    return filter_value(df, column_name, value, comparison_type)