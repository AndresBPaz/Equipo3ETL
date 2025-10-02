# 🌱 Equipo3ETL

📊 Este repositorio contiene el desarrollo de un proceso **ETL** (Extract, Transform, Load) diseñado para integrar, limpiar y unificar datos provenientes de diferentes fuentes utilizadas en la **agroindustria de la caña de azúcar**.

👥 **Integrantes**  
- Andres David Bolaños Paz  
- Santiago Correa Campaña  
- Jhonatan Andres Tapia Cordoba  
- Juan Jose Betancourt Osorio  

---

## 📑 Índice

- [⚙️ Instalación de dependencias](#️-instalación-de-dependencias)
- [📘 Descripción](#-descripción)
- [📂 Estructura](#-estructura)
- [🔧 Configuración](#-configuración)
- [🚀 Pipelines](#-pipelines)
- [▶️ Ejecución](#️-ejecución)
- [📤 Salidas](#-salidas)
- [🧩 Transformaciones](#-transformaciones)
- [📅 Fechas y formatos](#-fechas-y-formatos)
- [📎 Notas de uso de archivos](#-notas-de-uso-de-archivos)

---

## ⚙️ Instalación de dependencias

- Requisitos: **Python 3.8 o superior** (según `requires-python` en `pyproject.toml`).  
- Crear y activar un entorno virtual.  
- Instalar el proyecto desde la raíz para resolver automáticamente las dependencias de `pyproject.toml`.  

### 🔹 Pasos sugeridos:

**macOS/Linux**
```bash
python -m venv .venv
source .venv/bin/activate
```

**Windows**
```bash
python -m venv .venv
.venv\Scripts\activate
```

**Instalar el proyecto**
```bash
# Instalación editable (recomendada durante el desarrollo)
pip install -e .

# Instalación normal
pip install .
```

---

## 📘 Descripción

- Proyecto **ETL declarativo**: lectura, transformaciones y salidas por dataset definidas en `config/settings.yaml`.  
- Lectura de Excel con `pandas.read_excel` (engine=`openpyxl`) y exportación en **Parquet** y **CSV**.  
- Ejecución modular (por dataset) y con un **orquestador** para correr todos los flujos en secuencia.  

---

## 📂 Estructura

```text
etl-project/
├─ config/
│  └─ settings.yaml
├─ data/
│  ├─ raw/
│  │  ├─ abastecimientos/
│  │  ├─ actividades/
│  │  ├─ insumos/
│  │  └─ rep_maquinaria/
│  └─ processed/
├─ scripts/
│  ├─ run_abastecimientos.py
│  ├─ run_actividades.py
│  ├─ run_insumos.py
│  ├─ run_rep_maquina.py
│  └─ run_all.py
├─ src/
│  └─ etl_project/
│     ├─ loaders.py
│     ├─ transforms.py
│     └─ pipelines/
│        ├─ abastecimientos.py
│        ├─ actividades.py
│        ├─ insumos.py
│        └─ rep_maquinaria.py
├─ pyproject.toml
└─ README.md
```

---

## 🔧 Configuración

El archivo `config/settings.yaml` define:

- 📂 Paths base  
- 📑 Parámetros de lectura Excel (`engine`, `header`)  
- 📊 Datasets:  
  - **source** (carpeta, patrones)  
  - **transforms** (clean, drop, rename, filters, derive)  

📥 La lectura usa `pandas.read_excel` con `engine=openpyxl`.  
📤 Las salidas se escriben en `data/processed/` como **Parquet** y **CSV**.  

---

## 🚀 Pipelines

- **abastecimientos** → Limpieza de columnas, filtros declarados, renombrado y recorte de prefijos de equipo.  
- **actividades** → Limpieza, filtros, renombrado, ajuste de fechas y generación de IDs concatenados.  
- **insumos** → Limpieza, eliminación de campos, renombrado de fecha, derivación de hacienda e IDs.  
- **rep_maquinaria** → Limpieza, eliminación de campos no usados, renombrado de campos, ajuste de fechas e IDs.  

---

## ▶️ Ejecución

Ejecutar por dataset:

```bash
python scripts/run_abastecimientos.py
python scripts/run_actividades.py
python scripts/run_insumos.py
python scripts/run_rep_maquina.py
```

Ejecutar todo el flujo:

```bash
python scripts/run_all.py
```

Ejecutar carga a base de datos. 

```bash
python scripts/loadData.py
```
---

## 📤 Salidas

- **Parquet** → `data/processed/{dataset}.parquet` (requiere `pyarrow` o `fastparquet`).  
- **CSV** → `data/processed/{dataset}.csv` (se puede forzar el formato de fechas con `date_format`).  

---

## 🧩 Transformaciones

Funciones principales:

- `clean_column_names(df)` → estandariza nombres de columnas.  
- `drop_columns(df, cols)` → elimina columnas existentes (ignora las ausentes).  
- `filter_value(df, column, value, cmp)` → filtros (`equals`, `not_equals`, `in`, `between`, etc).  
- `delete_first_n(df, column, n)` → recorta caracteres iniciales.  
- `concat_columns(df, new_column, columns, sep, position)` → concatena columnas como string.  
- `concat_column_with_first_n(...)` → nueva columna a partir de primeros `n` caracteres.  
- `adjust_date_format(...)` → transforma fechas entre formatos.  

---

## 📅 Fechas y formatos

En la pipeline se recomienda:

```python
adjust_date_format(
    df,
    column_name="fecha",
    current_format="%d/%m/%Y %I:%M:%S %p",
    desired_format="%d/%m/%Y"
)
```

Al exportar CSV:

```python
df.to_csv("ruta.csv", index=False, encoding="utf-8", date_format="%d/%m/%Y")
```

---

Visualizar los datos. 
```python
./venv/bin/python './src/etl_project/views/plots.py'
streamlit run ./src/etl_project/views/plots.py
```