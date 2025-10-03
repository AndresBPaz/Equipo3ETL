"""
Dashboard Streamlit para análisis de agroindustria de caña de azúcar.
"""

import logging
import traceback
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from etl_project.config import Config
from etl_project.conexiondb import DatabaseConnection

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración de página
st.set_page_config(
    page_title="Dashboard Agroindustria Caña",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Aplicar tema personalizado a Plotly
pio.templates.default = "plotly_white"

# CSS personalizado
st.markdown("""
<style>
    .main > div {
        padding-top: 2rem;
    }
    .stSelectbox > label {
        font-weight: bold;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #1f77b4;
    }
</style>
""", unsafe_allow_html=True)


class DashboardError(Exception):
    """Excepción personalizada para errores del dashboard."""
    pass


@st.cache_data(ttl=600, show_spinner="🔄 Cargando datos...")
def load_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Carga los datos desde la base de datos con cache y manejo de errores.
    TTL de 10 minutos para refrescar datos automáticamente.
    """
    try:
        # Validar conexión antes de hacer queries
        config = Config("config/settings.yaml")
        db = DatabaseConnection()
        engine = db.get_engine()
        
        if engine is None:
            raise DashboardError("❌ No se pudo conectar a la base de datos")
        
        # Test de conexión
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        
        logger.info("✓ Conexión a base de datos establecida")
        
        # Definir queries con manejo de errores individuales
        queries = {
            "productividad_trabajador": "SELECT * FROM stage.vista_productividad_trabajador",
            "costo_insumos": "SELECT * FROM stage.vista_costo_insumos_por_hectarea", 
            "produccion_maquinaria": "SELECT * FROM stage.vista_produccion_maquinaria",
            "combustible_unidad": "SELECT * FROM stage.vista_combustible_por_unidad_producida"
        }
        
        dataframes = {}
        
        # Cargar cada dataset por separado con manejo individual de errores
        for name, query in queries.items():
            try:
                df = pd.read_sql(
                    query, 
                    con=engine,
                    chunksize=None  # Para datasets grandes, considerar chunking
                )
                
                if df.empty:
                    st.warning(f"⚠️ La vista '{name}' está vacía")
                    dataframes[name] = pd.DataFrame()
                else:
                    # Optimizar tipos de datos para reducir memoria
                    dataframes[name] = optimize_dtypes(df)
                    logger.info(f"✓ Cargados {len(df)} registros de {name}")
                    
            except SQLAlchemyError as e:
                logger.error(f"Error cargando {name}: {e}")
                st.error(f"❌ Error cargando datos de {name}: {e}")
                dataframes[name] = pd.DataFrame()
        
        return (
            dataframes.get("productividad_trabajador", pd.DataFrame()),
            dataframes.get("costo_insumos", pd.DataFrame()), 
            dataframes.get("produccion_maquinaria", pd.DataFrame()),
            dataframes.get("combustible_unidad", pd.DataFrame())
        )
        
    except Exception as e:
        logger.error(f"Error crítico cargando datos: {e}")
        logger.error(traceback.format_exc())
        st.error(f"❌ Error crítico: {str(e)}")
        
        # Retornar DataFrames vacíos en caso de error
        empty_df = pd.DataFrame()
        return empty_df, empty_df, empty_df, empty_df


def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Optimiza los tipos de datos del DataFrame para reducir uso de memoria."""
    if df.empty:
        return df
        
    optimized = df.copy()
    
    # Convertir columnas de fecha
    date_columns = [col for col in df.columns if 'fecha' in col.lower() or 'date' in col.lower()]
    for col in date_columns:
        try:
            optimized[col] = pd.to_datetime(optimized[col], errors='coerce')
        except:
            pass
    
    # Optimizar columnas numéricas
    for col in optimized.select_dtypes(include=['float64']).columns:
        optimized[col] = pd.to_numeric(optimized[col], downcast='float', errors='coerce')
    
    for col in optimized.select_dtypes(include=['int64']).columns:
        optimized[col] = pd.to_numeric(optimized[col], downcast='integer', errors='coerce')
    
    # Convertir strings a category si tienen pocos valores únicos
    for col in optimized.select_dtypes(include=['object']).columns:
        if len(optimized[col].unique()) / len(optimized) < 0.5:  # Menos del 50% valores únicos
            optimized[col] = optimized[col].astype('category')
    
    return optimized


def create_custom_theme() -> Dict[str, Any]:
    """Crea un tema personalizado para los gráficos."""
    return {
        'layout': {
            'colorway': ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
                        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'],
            'font': {'family': 'Arial, sans-serif', 'size': 12},
            'title': {'font': {'size': 16, 'color': '#2e2e2e'}},
            'plot_bgcolor': '#ffffff',
            'paper_bgcolor': '#ffffff',
        }
    }


def apply_filters(
    df: pd.DataFrame, 
    filters: Dict[str, List[str]]
) -> pd.DataFrame:
    """Aplica filtros al DataFrame de manera segura."""
    if df.empty:
        return df
        
    filtered_df = df.copy()
    
    for column, values in filters.items():
        if values and column in filtered_df.columns:
            filtered_df = filtered_df[filtered_df[column].isin(values)]
    
    return filtered_df


def create_productivity_chart(df: pd.DataFrame) -> Optional[go.Figure]:
    """Crea gráfico de productividad por actividad."""
    if df.empty or 'actividad' not in df.columns:
        st.warning("⚠️ No hay datos suficientes para mostrar productividad")
        return None
    
    df_plot = df.sort_values("total_producido", ascending=False)
    
    fig = px.bar(
        df_plot.head(20),  # Limitar a top 20 para mejor visualización
        x="actividad",
        y="total_producido", 
        color="actividad",
        hover_data=["empresa", "cantidad_registros", "promedio_produccion"] if all(
            col in df_plot.columns for col in ["empresa", "cantidad_registros", "promedio_produccion"]
        ) else None,
        title="📊 Total Producido por Actividad (Top 20)",
        template="plotly_white"
    )
    
    fig.update_layout(
        showlegend=False, 
        height=600,
        xaxis_title="Actividad",
        yaxis_title="Total Producido",
        title_x=0.5
    )
    fig.update_xaxes(tickangle=-45)
    
    return fig


def create_cost_chart(df: pd.DataFrame) -> Optional[go.Figure]:
    """Crea gráfico de costo por hectárea."""
    if df.empty or not all(col in df.columns for col in ['fecha', 'costo_por_hectarea']):
        st.warning("⚠️ No hay datos suficientes para mostrar costos")
        return None
    
    df_plot = df.sort_values("fecha")
    
    fig = px.line(
        df_plot,
        x="fecha",
        y="costo_por_hectarea",
        color="actividad" if "actividad" in df_plot.columns else None,
        hover_data=["costo_total", "area_total"] if all(
            col in df_plot.columns for col in ["costo_total", "area_total"]
        ) else None,
        title="💰 Evolución del Costo por Hectárea",
        template="plotly_white"
    )
    
    fig.update_layout(
        height=450,
        xaxis_title="Fecha",
        yaxis_title="Costo por Hectárea",
        title_x=0.5
    )
    
    return fig


def create_machinery_chart(df: pd.DataFrame) -> Optional[go.Figure]:
    """Crea gráfico de producción por maquinaria."""
    if df.empty or not all(col in df.columns for col in ['nombre_actividad', 'produccion_por_hora']):
        st.warning("⚠️ No hay datos suficientes para mostrar producción de maquinaria")
        return None
    
    df_plot = df.sort_values("produccion_por_hora", ascending=False)
    
    fig = px.bar(
        df_plot,
        x="nombre_actividad",
        y="produccion_por_hora", 
        color="nombre_actividad",
        hover_data=["total_produccion", "total_horas"] if all(
            col in df_plot.columns for col in ["total_produccion", "total_horas"]
        ) else None,
        title="🚜 Producción por Hora por Actividad de Maquinaria",
        template="plotly_white"
    )
    
    fig.update_layout(
        height=450, 
        showlegend=False,
        xaxis_title="Actividad",
        yaxis_title="Producción por Hora",
        title_x=0.5
    )
    
    return fig


def create_fuel_chart(df: pd.DataFrame) -> Optional[go.Figure]:
    """Crea gráfico de combustible por unidad producida."""
    if df.empty or not all(col in df.columns for col in ['mes', 'galones_por_unidad']):
        st.warning("⚠️ No hay datos suficientes para mostrar consumo de combustible")
        return None
    
    df_plot = df.sort_values("mes").copy()
    df_plot["galones_por_unidad"] = df_plot["galones_por_unidad"].abs()
    
    fig = px.line(
        df_plot,
        x="mes",
        y="galones_por_unidad",
        hover_data=["total_galones", "total_produccion"] if all(
            col in df_plot.columns for col in ["total_galones", "total_produccion"] 
        ) else None,
        title="⛽ Galones de Combustible por Unidad Producida",
        template="plotly_white"
    )
    
    fig.update_traces(mode="lines+markers", marker=dict(size=8), line=dict(width=3))
    fig.update_layout(
        height=450,
        showlegend=False,
        xaxis_title="Mes", 
        yaxis_title="Galones por Unidad",
        title_x=0.5
    )
    
    return fig


def show_summary_metrics(
    df_prod: pd.DataFrame, 
    df_cost: pd.DataFrame, 
    df_mach: pd.DataFrame, 
    df_fuel: pd.DataFrame
) -> None:
    """Muestra métricas resumidas en la parte superior."""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_prod = df_prod["total_producido"].sum() if not df_prod.empty and "total_producido" in df_prod.columns else 0
        st.metric(
            label="🎯 Producción Total", 
            value=f"{total_prod:,.0f}",
            help="Suma total de producción de todos los trabajadores"
        )
    
    with col2:
        avg_cost = df_cost["costo_por_hectarea"].mean() if not df_cost.empty and "costo_por_hectarea" in df_cost.columns else 0
        st.metric(
            label="💰 Costo Promedio/Ha",
            value=f"${avg_cost:,.2f}",
            help="Costo promedio por hectárea de insumos"
        )
    
    with col3:
        total_activities = len(df_mach) if not df_mach.empty else 0
        st.metric(
            label="🚜 Actividades Maquinaria",
            value=f"{total_activities}",
            help="Número total de actividades de maquinaria registradas"
        )
    
    with col4:
        avg_fuel = df_fuel["galones_por_unidad"].mean() if not df_fuel.empty and "galones_por_unidad" in df_fuel.columns else 0
        st.metric(
            label="⛽ Combustible Promedio",
            value=f"{abs(avg_fuel):.2f} gal/unidad",
            help="Consumo promedio de combustible por unidad producida"
        )


def main():
    """Función principal del dashboard."""
    try:
        # Título principal
        st.title("🌱 Dashboard Agroindustria Caña")
        st.markdown("---")
        
        # Cargar datos
        with st.spinner("🔄 Cargando datos del sistema..."):
            df_prod_trab, df_costo_ha, df_prod_maq, df_comb_unit = load_data()
        
        # Verificar si hay datos
        if all(df.empty for df in [df_prod_trab, df_costo_ha, df_prod_maq, df_comb_unit]):
            st.error("❌ No se pudieron cargar los datos. Verificar conexión a la base de datos.")
            return
        
        # Mostrar métricas de resumen
        show_summary_metrics(df_prod_trab, df_costo_ha, df_prod_maq, df_comb_unit)
        st.markdown("---")
        
        # Panel de filtros en sidebar
        with st.sidebar:
            st.header("🔍 Filtros")
            
            # Filtros condicionales basados en datos disponibles
            filters = {}
            
            if not df_prod_trab.empty and "trabajador" in df_prod_trab.columns:
                filters["trabajador"] = st.multiselect(
                    "👷 Trabajador", 
                    sorted(df_prod_trab["trabajador"].unique()),
                    help="Filtrar por trabajador específico"
                )
            
            if not df_prod_trab.empty and "actividad" in df_prod_trab.columns:
                filters["actividad_trab"] = st.multiselect(
                    "⚡ Actividad (trabajador)", 
                    sorted(df_prod_trab["actividad"].unique())
                )
            
            if not df_costo_ha.empty and "hacienda" in df_costo_ha.columns:
                filters["hacienda"] = st.multiselect(
                    "🏡 Hacienda", 
                    sorted(df_costo_ha["hacienda"].unique())
                )
            
            if not df_costo_ha.empty and "actividad" in df_costo_ha.columns:
                filters["actividad_insumo"] = st.multiselect(
                    "🌿 Actividad (insumos)", 
                    sorted(df_costo_ha["actividad"].unique())
                )
            
            if not df_prod_maq.empty and "nombre_actividad" in df_prod_maq.columns:
                filters["actividad_maquina"] = st.multiselect(
                    "🚜 Actividad maquinaria", 
                    sorted(df_prod_maq["nombre_actividad"].unique())
                )
            
            if not df_comb_unit.empty and "mes" in df_comb_unit.columns:
                filters["mes"] = st.multiselect(
                    "📅 Mes", 
                    sorted(df_comb_unit["mes"].unique(), reverse=True)
                )
            
            # Botón para limpiar filtros
            if st.button("🗑️ Limpiar Filtros"):
                st.experimental_rerun()
        
        # Aplicar filtros
        if filters.get("trabajador"):
            df_prod_trab = df_prod_trab[df_prod_trab["trabajador"].isin(filters["trabajador"])]
        
        if filters.get("actividad_trab"):
            df_prod_trab = df_prod_trab[df_prod_trab["actividad"].isin(filters["actividad_trab"])]
        
        if filters.get("hacienda"):
            df_costo_ha = df_costo_ha[df_costo_ha["hacienda"].isin(filters["hacienda"])]
        
        if filters.get("actividad_insumo"):
            df_costo_ha = df_costo_ha[df_costo_ha["actividad"].isin(filters["actividad_insumo"])]
        
        if filters.get("actividad_maquina"):
            df_prod_maq = df_prod_maq[df_prod_maq["nombre_actividad"].isin(filters["actividad_maquina"])]
        
        if filters.get("mes"):
            df_comb_unit = df_comb_unit[df_comb_unit["mes"].isin(filters["mes"])]
        
        # Crear gráficos
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Productividad por Actividad")
            fig1 = create_productivity_chart(df_prod_trab)
            if fig1:
                st.plotly_chart(fig1, use_container_width=True, key="prod_chart")
        
        with col2:
            st.subheader("💰 Costo de Insumos por Hectárea") 
            fig2 = create_cost_chart(df_costo_ha)
            if fig2:
                st.plotly_chart(fig2, use_container_width=True, key="cost_chart")
        
        st.markdown("---")
        
        col3, col4 = st.columns(2)
        
        with col3:
            st.subheader("🚜 Producción por Maquinaria")
            fig3 = create_machinery_chart(df_prod_maq) 
            if fig3:
                st.plotly_chart(fig3, use_container_width=True, key="mach_chart")
        
        with col4:
            st.subheader("⛽ Combustible por Unidad Producida")
            fig4 = create_fuel_chart(df_comb_unit)
            if fig4:
                st.plotly_chart(fig4, use_container_width=True, key="fuel_chart")
        
        # Información adicional
        with st.expander("ℹ️ Información del Dataset"):
            st.write("**Resumen de datos cargados:**")
            
            data_summary = {
                "Dataset": ["Productividad Trabajador", "Costo Insumos", "Producción Maquinaria", "Combustible"],
                "Registros": [len(df_prod_trab), len(df_costo_ha), len(df_prod_maq), len(df_comb_unit)],
                "Estado": ["✅ Cargado" if len(df) > 0 else "⚠️ Vacío" 
                          for df in [df_prod_trab, df_costo_ha, df_prod_maq, df_comb_unit]]
            }
            
            st.dataframe(pd.DataFrame(data_summary), use_container_width=True)
    
    except Exception as e:
        logger.error(f"Error en función principal: {e}")
        logger.error(traceback.format_exc())
        st.error(f"❌ Error inesperado: {str(e)}")
        st.error("Por favor, contactar al administrador del sistema.")


if __name__ == "__main__":
    main()
