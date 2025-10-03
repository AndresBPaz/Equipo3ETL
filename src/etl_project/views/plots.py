
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Dashboard Caña", layout="wide")

# Configuración y conexión para Streamlit Cloud
@st.cache_resource
def get_database_connection():
    """Obtiene y cachea la conexión a la base de datos usando st.connection."""
    try:
        # Usar st.connection para Streamlit Cloud
        conn = st.connection('postgresql', type='sql')
        
        # Test de conexión
        test_df = conn.query("SELECT 1 as test", ttl=0)
        st.success("✅ Conectado a la base de datos")
        
        return conn
    except Exception as e:
        st.error(f"❌ Error de conexión: {e}")
        st.info("💡 Verificar que los secrets estén configurados correctamente en Streamlit Cloud")
        st.stop()

# Obtener conexión
conn = get_database_connection()

# Cargar vistas
@st.cache_data(show_spinner="🔄 Cargando datos...")
def load_data():
    """Carga los datos desde las vistas de la base de datos."""
    try:
        # TTL de 300 segundos (5 minutos) para cache
        df_prod_trab = conn.query("SELECT * FROM stage.vista_productividad_trabajador", ttl=300)
        df_costo_ha = conn.query("SELECT * FROM stage.vista_costo_insumos_por_hectarea", ttl=300)
        df_prod_maq = conn.query("SELECT * FROM stage.vista_produccion_maquinaria", ttl=300)
        df_comb_unit = conn.query("SELECT * FROM stage.vista_combustible_por_unidad_producida", ttl=300)
        
        return df_prod_trab, df_costo_ha, df_prod_maq, df_comb_unit
    except Exception as e:
        st.error(f"❌ Error cargando datos: {e}")
        empty_df = pd.DataFrame()
        return empty_df, empty_df, empty_df, empty_df

# Cargar datos
df_prod_trab, df_costo_ha, df_prod_maq, df_comb_unit = load_data()

# Validar si hay datos antes de continuar
if all(df.empty for df in [df_prod_trab, df_costo_ha, df_prod_maq, df_comb_unit]):
    st.error("❌ No hay datos disponibles. Verificar las vistas en la base de datos.")
    st.stop()

# Título principal
st.title("🌱 Dashboard Agroindustria Caña")

# Panel de filtros en sidebar
with st.sidebar:
    st.header("🔍 Filtros")
    
    # Filtro de trabajador
    trabajador_sel = []
    if not df_prod_trab.empty and "trabajador" in df_prod_trab.columns:
        trabajador_sel = st.multiselect(
            "👷 Trabajador", 
            sorted(df_prod_trab["trabajador"].unique())
        )
    
    # Filtro de actividad trabajador
    actividad_trab_sel = []
    if not df_prod_trab.empty and "actividad" in df_prod_trab.columns:
        actividad_trab_sel = st.multiselect(
            "⚡ Actividad (trabajador)", 
            sorted(df_prod_trab["actividad"].unique())
        )
    
    # Filtro de hacienda
    hacienda_sel = []
    if not df_costo_ha.empty and "hacienda" in df_costo_ha.columns:
        hacienda_sel = st.multiselect(
            "🏡 Hacienda", 
            sorted(df_costo_ha["hacienda"].unique())
        )
    
    # Filtro de actividad insumos
    actividad_insumo_sel = []
    if not df_costo_ha.empty and "actividad" in df_costo_ha.columns:
        actividad_insumo_sel = st.multiselect(
            "🌿 Actividad (insumos)", 
            sorted(df_costo_ha["actividad"].unique())
        )
    
    # Filtro de actividad maquinaria
    actividad_maquina_sel = []
    if not df_prod_maq.empty and "nombre_actividad" in df_prod_maq.columns:
        actividad_maquina_sel = st.multiselect(
            "🚜 Actividad maquinaria", 
            sorted(df_prod_maq["nombre_actividad"].unique())
        )
    
    # Filtro de mes
    mes_sel = []
    if not df_comb_unit.empty and "mes" in df_comb_unit.columns:
        mes_sel = st.multiselect(
            "📅 Mes", 
            sorted(df_comb_unit["mes"].unique(), reverse=True)
        )

# Aplicar filtros
if trabajador_sel and not df_prod_trab.empty:
    df_prod_trab = df_prod_trab[df_prod_trab["trabajador"].isin(trabajador_sel)]

if actividad_trab_sel and not df_prod_trab.empty:
    df_prod_trab = df_prod_trab[df_prod_trab["actividad"].isin(actividad_trab_sel)]

if hacienda_sel and not df_costo_ha.empty:
    df_costo_ha = df_costo_ha[df_costo_ha["hacienda"].isin(hacienda_sel)]

if actividad_insumo_sel and not df_costo_ha.empty:
    df_costo_ha = df_costo_ha[df_costo_ha["actividad"].isin(actividad_insumo_sel)]

if actividad_maquina_sel and not df_prod_maq.empty:
    df_prod_maq = df_prod_maq[df_prod_maq["nombre_actividad"].isin(actividad_maquina_sel)]

if mes_sel and not df_comb_unit.empty:
    df_comb_unit = df_comb_unit[df_comb_unit["mes"].isin(mes_sel)]

# Gráfico 1: Productividad por actividad
if not df_prod_trab.empty and "total_producido" in df_prod_trab.columns and "actividad" in df_prod_trab.columns:
    st.subheader("📊 Productividad por actividad")
    
    df_prod_trab_plot = df_prod_trab.sort_values("total_producido", ascending=False)
    
    # Verificar qué columnas existen para hover_data
    hover_cols = []
    for col in ["empresa", "cantidad_registros", "promedio_produccion"]:
        if col in df_prod_trab_plot.columns:
            hover_cols.append(col)
    
    fig1 = px.bar(
        df_prod_trab_plot,
        x="actividad",
        y="total_producido", 
        color="actividad",
        hover_data=hover_cols if hover_cols else None,
        title="Total producido por actividad",
        category_orders={"actividad": df_prod_trab_plot["actividad"].tolist()},
    )
    
    fig1.update_layout(showlegend=False, height=600)
    fig1.update_xaxes(tickangle=-45)
    st.plotly_chart(fig1, use_container_width=True)
else:
    st.warning("⚠️ No hay datos de productividad disponibles")

st.divider()

# Gráfico 2: Costo de insumos por hectárea
if not df_costo_ha.empty and all(col in df_costo_ha.columns for col in ["fecha", "costo_por_hectarea"]):
    st.subheader("💰 Costo de insumos por hectárea")
    
    df_costo_ha_plot = df_costo_ha.sort_values("fecha")
    
    # Verificar columnas para hover_data
    hover_cols = []
    for col in ["actividad", "costo_total", "area_total"]:
        if col in df_costo_ha_plot.columns:
            hover_cols.append(col)
    
    fig2 = px.line(
        df_costo_ha_plot,
        x="fecha",
        y="costo_por_hectarea",
        color="actividad" if "actividad" in df_costo_ha_plot.columns else None,
        hover_data=hover_cols if hover_cols else None,
        title="Costo por hectárea a lo largo del tiempo",
    )
    
    fig2.update_layout(height=450)
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.warning("⚠️ No hay datos de costos disponibles")

st.divider()

# Gráfico 3: Producción por maquinaria
if not df_prod_maq.empty and all(col in df_prod_maq.columns for col in ["nombre_actividad", "produccion_por_hora"]):
    st.subheader("🚜 Producción por maquinaria")
    
    df_prod_maq_plot = df_prod_maq.sort_values("produccion_por_hora", ascending=False)
    
    # Verificar columnas para hover_data
    hover_cols = []
    for col in ["total_produccion", "total_horas"]:
        if col in df_prod_maq_plot.columns:
            hover_cols.append(col)
    
    fig3 = px.bar(
        df_prod_maq_plot,
        x="nombre_actividad",
        y="produccion_por_hora", 
        color="nombre_actividad",
        hover_data=hover_cols if hover_cols else None,
        title="Producción por hora por actividad",
    )
    
    fig3.update_layout(height=450, showlegend=False, xaxis_title="Actividad")
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.warning("⚠️ No hay datos de maquinaria disponibles")

st.divider()

# Gráfico 4: Combustible por unidad producida
if not df_comb_unit.empty and all(col in df_comb_unit.columns for col in ["mes", "galones_por_unidad"]):
    st.subheader("⛽ Combustible por unidad producida (mensual)")
    
    df_comb_unit_plot = df_comb_unit.sort_values("mes").copy()
    df_comb_unit_plot["galones_por_unidad"] = df_comb_unit_plot["galones_por_unidad"].abs()
    
    # Verificar columnas para hover_data
    hover_cols = []
    for col in ["total_galones", "total_produccion"]:
        if col in df_comb_unit_plot.columns:
            hover_cols.append(col)
    
    fig4 = px.line(
        df_comb_unit_plot,
        x="mes",
        y="galones_por_unidad",
        hover_data=hover_cols if hover_cols else None,
        title="Galones por unidad producida",
    )
    
    fig4.update_traces(mode="lines+markers", marker=dict(size=8), line=dict(width=3))
    fig4.update_layout(height=450, showlegend=False, xaxis_title="Mes")
    st.plotly_chart(fig4, use_container_width=True)
else:
    st.warning("⚠️ No hay datos de combustible disponibles")

# Footer con información
st.divider()
with st.expander("ℹ️ Información del Dashboard"):
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Registros Productividad", len(df_prod_trab))
    with col2:
        st.metric("Registros Costos", len(df_costo_ha))
    with col3:
        st.metric("Registros Maquinaria", len(df_prod_maq))
    with col4:
        st.metric("Registros Combustible", len(df_comb_unit))
