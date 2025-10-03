# 🌱 Equipo3ETL

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-green.svg)](https://www.postgresql.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.28+-red.svg)](https://streamlit.io/)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-orange.svg)](https://github.com/features/actions)

📊 Sistema ETL completo para integrar, limpiar y unificar datos provenientes de diferentes fuentes utilizadas en la **agroindustria de la caña de azúcar**.

> 🎯 **Proyecto desarrollado como parte del curso de ETL - 2025**

---

## 👥 Integrantes

- **Andres David Bolaños Paz** - [@AndresBPaz](https://github.com/AndresBPaz)
- **Santiago Correa Campaña**
- **Jhonatan Andres Tapia Cordoba** - [@JhonatanTC99](https://github.com/JhonatanTC99)
- **Juan Jose Betancourt Osorio** - [@juanjo44](https://github.com/juanjo44)

---

## 📑 Tabla de Contenidos

- [✨ Características](#-características)
- [🏗️ Arquitectura](#️-arquitectura)
- [⚙️ Instalación](#️-instalación)
- [📂 Estructura del Proyecto](#-estructura-del-proyecto)
- [🔧 Configuración](#-configuración)
- [🚀 Uso](#-uso)
- [📊 Dashboard](#-dashboard)
- [🔄 CI/CD con GitHub Actions](#-cicd-con-github-actions)
- [🧩 Transformaciones](#-transformaciones)
- [🗄️ Base de Datos](#️-base-de-datos)

---

## ✨ Características

### Pipeline ETL Completo

- ✅ **Extracción**: Lectura de múltiples archivos Excel (.xlsx, .xlsm)
- ✅ **Transformación**: Limpieza, filtrado y normalización de datos declarativa
- ✅ **Carga**: Almacenamiento en PostgreSQL con esquema normalizado
- ✅ **Exportación**: Salidas en formato Parquet y CSV

### Datasets Procesados

1. **Abastecimientos** - Consumo de combustible por equipo
2. **Actividades** - Productividad de trabajadores por actividad
3. **Insumos** - Costos de insumos por hectárea
4. **Reporte Maquinaria** - Eficiencia de equipos y horas de trabajo

### Dashboard Interactivo

- 📈 Visualizaciones con Plotly Express
- 🔍 Filtros dinámicos y reactivos
- ⚡ Cache inteligente con actualización automática
- 📱 Diseño responsive

### Automatización

- 🤖 CI/CD con GitHub Actions
- 🐘 PostgreSQL como servicio en tests
- ✅ Validación automática de datos

---

## 🏗️ Arquitectura

```

┌─────────────────┐
│  Excel Files    │
│  (.xlsx/.xlsm)  │
└────────┬────────┘
│
▼
┌─────────────────┐
│   Extract       │◄─── loaders.py
│   (pandas)      │
└────────┬────────┘
│
▼
┌─────────────────┐
│   Transform     │◄─── transforms.py
│   (clean/filter)│      + settings.yaml
└────────┬────────┘
│
├─────► Parquet Files
│
├─────► CSV Files
│
▼
┌─────────────────┐
│   Load          │◄─── CSVLoader.py
│   (PostgreSQL)  │      + conexiondb.py
└────────┬────────┘
│
▼
┌─────────────────┐
│   Visualize     │◄─── plots.py
│   (Streamlit)   │      + Plotly
└─────────────────┘

```

---

## ⚙️ Instalación

### Requisitos Previos

- **Python 3.8+**
- **PostgreSQL 12+** (opcional, solo para carga a DB)
- **Git**

### Pasos de Instalación

1. **Clonar el repositorio**

```

git clone https://github.com/AndresBPaz/Equipo3ETL.git
cd Equipo3ETL

```

2. **Crear y activar entorno virtual**

**macOS/Linux:**
```

python -m venv .venv
source .venv/bin/activate

```

**Windows:**
```

python -m venv .venv
.venv\Scripts\activate

```

3. **Instalar dependencias**

```

pip install -e .

```

4. **Configurar variables de entorno** (para desarrollo local con DB)

Crear archivo `.env`:

```

DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydb
DB_USER=postgres
DB_PASSW=tu_password

```

---

## 📂 Estructura del Proyecto

```

Equipo3ETL/
├── .github/
│   └── workflows/
│       └── etl.yml              \# CI/CD con GitHub Actions
├── .streamlit/
│   └── secrets.toml            \# Secrets para Streamlit (no subir a Git)
├── config/
│   └── settings.yaml           \# Configuración declarativa del ETL
├── data/
│   ├── raw/                    \# Archivos Excel fuente
│   │   ├── abastecimientos/
│   │   ├── actividades/
│   │   ├── insumos/
│   │   └── rep_maquinaria/
│   └── processed/              \# Archivos transformados (Parquet/CSV)
├── scripts/
│   ├── run_all.py             \# Ejecutar todos los pipelines
│   ├── loadData.py            \# Cargar datos a PostgreSQL
│   ├── run_abastecimientos.py
│   ├── run_actividades.py
│   ├── run_insumos.py
│   └── run_rep_maquina.py
├── src/
│   └── etl_project/
│       ├── config.py          \# Gestión de configuración
│       ├── conexiondb.py      \# Conexión PostgreSQL con reintentos
│       ├── CSVLoader.py       \# Carga de CSVs a base de datos
│       ├── loaders.py         \# Lectura de archivos Excel
│       ├── transforms.py      \# Funciones de transformación
│       ├── pipelines/         \# Pipelines por dataset
│       │   ├── abastecimientos.py
│       │   ├── actividades.py
│       │   ├── insumos.py
│       │   └── rep_maquinaria.py
│       └── views/
│           └── plots.py       \# Dashboard Streamlit
├── .gitignore
├── pyproject.toml             \# Dependencias y metadata
└── README.md

```

---

## 🔧 Configuración

### Archivo `config/settings.yaml`

Define toda la configuración del ETL de manera declarativa:

```

database:
HOST: localhost
PORT: 5432
DB_NAME: mydb
USER: postgres
PASSWORD: ""

paths:
data_raw: "data/raw/"
data_processed: "data/processed/"

datasets:
abastecimientos:
source:
folder: "data/raw/abastecimientos/"
patterns: ["*.xlsx", "*.xlsm"]
transforms:
clean_columns: true
drop_columns: [material, texto_breve_de_material]
filters:
- { column: clase_de_movimiento, op: equals, value: 261 }
rename:
"fe.contabilización": fecha
orden: equipo
derive:
delete_first_n:
column: equipo
n: 3

```

### Prioridad de Variables

El sistema carga configuración en este orden:

1. **Variables de entorno** (`os.environ`)
2. **Streamlit secrets** (`st.secrets`)
3. **Archivo YAML** (`config/settings.yaml`)

---

## 🚀 Uso

### 1. Ejecutar Pipeline Completo

Procesar todos los datasets:

```

python scripts/run_all.py

```

Esto genera archivos en `data/processed/`:
- `abastecimientos.parquet` / `abastecimientos.csv`
- `actividades.parquet` / `actividades.csv`
- `insumos.parquet` / `insumos.csv`
- `rep_maquinaria.parquet` / `rep_maquinaria.csv`

### 2. Ejecutar Pipeline Individual

```

python scripts/run_abastecimientos.py
python scripts/run_actividades.py
python scripts/run_insumos.py
python scripts/run_rep_maquina.py

```

### 3. Cargar Datos a PostgreSQL

**Requisito**: PostgreSQL corriendo y credenciales configuradas.

```

python scripts/loadData.py

```

### 4. Ejecutar Dashboard Localmente

```

streamlit run src/etl_project/views/plots.py

```

Dashboard disponible en: `http://localhost:8501`

---

## 📊 Dashboard

### Visualizaciones Disponibles

1. **📈 Productividad por Actividad**
   - Gráfico de barras con total producido
   - Filtros: trabajador, actividad
   - Hover: empresa, cantidad registros, promedio

2. **💰 Costo de Insumos por Hectárea**
   - Gráfico de líneas temporal
   - Filtros: hacienda, actividad
   - Hover: costo total, área total

3. **🚜 Producción por Maquinaria**
   - Gráfico de barras por actividad
   - Filtros: actividad maquinaria
   - Hover: producción total, horas totales

4. **⛽ Combustible por Unidad Producida**
   - Gráfico de líneas mensual
   - Filtros: mes
   - Hover: galones totales, producción total

### Desplegar en Streamlit Community Cloud

1. **Crear `.streamlit/secrets.toml`** (local, no subir a Git):

```

[connections.postgresql]
dialect = "postgresql"
host = "tu-host.com"
port = 5432
database = "mydb"
username = "tu_usuario"
password = "tu_password"

```

2. **Conectar repositorio en Streamlit Cloud**

3. **Configurar secrets** en el panel de Streamlit:
   - Settings > Secrets
   - Pegar contenido de `secrets.toml`

4. **Deploy automático** ✅

---

## 🔄 CI/CD con GitHub Actions

El workflow `.github/workflows/etl.yml` ejecuta automáticamente:

### En cada Push/PR a `main`:

✅ Configura PostgreSQL como servicio
✅ Instala dependencias Python
✅ Ejecuta pipeline de transformación (`run_all.py`)
✅ Carga datos a PostgreSQL de prueba (`loadData.py`)
✅ Valida conexiones y esquemas

### Configuración del Workflow

```

services:
postgres:
image: postgres:15
env:
POSTGRES_USER: testuser
POSTGRES_PASSWORD: testpass
POSTGRES_DB: mydb
ports:
- 5432:5432
options: >-
--health-cmd "pg_isready"
--health-interval 10s
--health-timeout 5s
--health-retries 10

```

---

## 🧩 Transformaciones

### Funciones Disponibles

| Función | Descripción | Ejemplo |
|---------|-------------|---------|
| `clean_column_names(df)` | Normaliza nombres de columnas | `fecha_inicio` |
| `drop_columns(df, cols)` | Elimina columnas | `['col1', 'col2']` |
| `filter_value(df, col, val, op)` | Filtros condicionales | `op='equals'` |
| `delete_first_n(df, col, n)` | Recorta caracteres iniciales | `n=3` |
| `concat_columns(df, new, cols, sep)` | Concatena columnas | `sep='_'` |
| `adjust_date_format(df, col, fmt_in, fmt_out)` | Transforma fechas | `'%d/%m/%Y'` |

### Operadores de Filtro

- `equals`: Igualdad exacta
- `not_equals`: Diferente de
- `in`: Contenido en lista
- `not_in`: No contenido en lista
- `greater_than`: Mayor que
- `less_than`: Menor que
- `between`: Entre dos valores

### Ejemplo de Uso

```

from etl_project.transforms import clean_column_names, filter_value

# Limpiar nombres

df = clean_column_names(df)

# Filtrar movimientos tipo 261

df = filter_value(df, "clase_de_movimiento", 261, "equals")

# Filtrar centros de costo < 20000000

df = filter_value(df, "centro_de_coste", 20000000, "less_than")

```

---

## 🗄️ Base de Datos

### Esquema PostgreSQL

```

-- Esquema raw: datos originales
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.abastecimientos (...);
CREATE TABLE raw.actividades (...);
CREATE TABLE raw.insumos (...);
CREATE TABLE raw.rep_maquinaria (...);

-- Esquema stage: vistas analíticas
CREATE SCHEMA IF NOT EXISTS stage;

CREATE VIEW stage.vista_productividad_trabajador AS
SELECT
trabajador,
actividad,
empresa,
SUM(produccion) as total_producido,
COUNT(*) as cantidad_registros,
AVG(produccion) as promedio_produccion
FROM raw.actividades
GROUP BY trabajador, actividad, empresa;

-- Otras vistas...

```

### Conexión a la Base de Datos

```

from etl_project.conexiondb import DatabaseConnection
import pandas as pd

# Obtener conexión

db = DatabaseConnection()
engine = db.get_engine()

# Ejecutar query

df = pd.read_sql("SELECT * FROM raw.abastecimientos LIMIT 10", con=engine)

# Cerrar conexión

db.close()

```

---

## 📝 Mejores Prácticas

### Desarrollo Local

1. Usar entorno virtual siempre
2. No subir `data/` a Git (agregado a `.gitignore`)
3. Probar pipelines individualmente antes de `run_all.py`
4. Validar salidas en `data/processed/`

### Producción

1. Configurar secrets correctamente
2. Validar healthchecks de PostgreSQL
3. Monitorear logs de GitHub Actions
4. Documentar cambios en configuración YAML

---

## 🤝 Contribuir

### Workflow

1. Fork el repositorio
2. Crear rama: `git checkout -b feature/nueva-funcionalidad`
3. Commit: `git commit -m 'Agrega nueva funcionalidad'`
4. Push: `git push origin feature/nueva-funcionalidad`
5. Abrir Pull Request

### Estándares

- ✅ Usar type hints en funciones
- ✅ Documentar con docstrings
- ✅ Seguir PEP 8
- ✅ Agregar tests para nuevas features

---

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver [LICENSE](LICENSE) para detalles.

---

## 🙏 Agradecimientos

- Universidad por el apoyo académico
- Comunidad open-source por las herramientas
- Instructores por la guía en el desarrollo ETL

---

## 📞 Contacto

**Andres Bolaños** - [@AndresBPaz](https://github.com/AndresBPaz)

---

<div align="center">

⭐ **Si este proyecto te fue útil, dale una estrella** ⭐

Hecho con ❤️ por el Equipo 3

</div>