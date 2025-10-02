# app.py
import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine
from etl_project.config import Config
from etl_project.conexiondb import DatabaseConnection

st.set_page_config(page_title="Dashboard Caña", layout="wide")

PGUSER = os.getenv("DB_USER") or os.getenv("PGUSER") or st.secrets.get("DB_USER")
PGPASSWORD = os.getenv("DB_PASSWORD") or os.getenv("PGPASSWORD") or st.secrets.get("DB_PASSW")
PGHOST = os.getenv("DB_HOST") or os.getenv("PGHOST") or st.secrets.get("DB_HOST")
PGPORT = os.getenv("DB_PORT") or os.getenv("PGPORT") or st.secrets.get("DB_PORT", "5432")
PGDATABASE = os.getenv("DB_NAME") or os.getenv("PGDATABASE") or st.secrets.get("DB_NAME")


db = DatabaseConnection()
engine = db.get_engine()

#engine = create_engine(f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}")

# Cargar vistas
@st.cache_data(show_spinner=False)
def load_data():
    df_prod_trab = pd.read_sql("select * from stage.vista_productividad_trabajador", con=engine)
    df_costo_ha  = pd.read_sql("select * from stage.vista_costo_insumos_por_hectarea", con=engine)
    df_prod_maq  = pd.read_sql("select * from stage.vista_produccion_maquinaria", con=engine)
    df_comb_unit = pd.read_sql("select * from stage.vista_combustible_por_unidad_producida", con=engine)
    return df_prod_trab, df_costo_ha, df_prod_maq, df_comb_unit

df_prod_trab, df_costo_ha, df_prod_maq, df_comb_unit = load_data()

st.title("Dashboard Agroindustria Caña")

# Filtros
with st.sidebar:
    st.header("Filtros")
    trabajador_sel = st.multiselect("Trabajador", sorted(df_prod_trab["trabajador"].unique()))
    actividad_trab_sel = st.multiselect("Actividad (trabajador)", sorted(df_prod_trab["actividad"].unique()))
    hacienda_sel = st.multiselect("Hacienda", sorted(df_costo_ha["hacienda"].unique()))
    actividad_insumo_sel = st.multiselect("Actividad (insumos)", sorted(df_costo_ha["actividad"].unique()))
    actividad_maquina_sel = st.multiselect("Actividad maquinaria", sorted(df_prod_maq["nombre_actividad"].unique()))
    mes_sel = st.multiselect("Mes", sorted(df_comb_unit["mes"].unique(), reverse=True))

# Aplicar filtros simples
if trabajador_sel:
    df_prod_trab = df_prod_trab[df_prod_trab["trabajador"].isin(trabajador_sel)]
if actividad_trab_sel:
    df_prod_trab = df_prod_trab[df_prod_trab["actividad"].isin(actividad_trab_sel)]
if hacienda_sel:
    df_costo_ha = df_costo_ha[df_costo_ha["hacienda"].isin(hacienda_sel)]
if actividad_insumo_sel:
    df_costo_ha = df_costo_ha[df_costo_ha["actividad"].isin(actividad_insumo_sel)]
if actividad_maquina_sel:
    df_prod_maq = df_prod_maq[df_prod_maq["nombre_actividad"].isin(actividad_maquina_sel)]
if mes_sel:
    df_comb_unit = df_comb_unit[df_comb_unit["mes"].isin(mes_sel)]

st.subheader("Productividad por actividad")
df_prod_trab_plot = df_prod_trab.sort_values("total_producido", ascending=False)
fig1 = px.bar(
    df_prod_trab_plot,
    x="actividad",
    y="total_producido",
    color="actividad",
    hover_data=["empresa", "cantidad_registros", "promedio_produccion"],
    title="Total producido por actividad",
    category_orders={"actividad": df_prod_trab_plot["actividad"].tolist()},
)
fig1.update_layout(showlegend=False, height=600)
fig1.update_xaxes(tickangle=-45)
st.plotly_chart(fig1, use_container_width=True)

st.divider()

st.subheader("Costo de insumos por hectárea")
df_costo_ha_plot = df_costo_ha.sort_values("fecha")
fig2 = px.line(
    df_costo_ha_plot,
    x="fecha",
    y="costo_por_hectarea",
    color="actividad",
    hover_data=["actividad", "costo_total", "area_total"],
    title="Costo por hectárea a lo largo del tiempo",
)
fig2.update_layout(height=450)
st.plotly_chart(fig2, use_container_width=True)

st.divider()

st.subheader("Producción por maquinaria")
df_prod_maq_plot = df_prod_maq.sort_values("produccion_por_hora", ascending=False)
fig3 = px.bar(
    df_prod_maq_plot,
    x="nombre_actividad",
    y="produccion_por_hora",
    color="nombre_actividad",
    hover_data=["total_produccion", "total_horas"],
    title="Producción por hora por actividad",
)
fig3.update_layout(height=450, showlegend=False, xaxis_title="Actividad")
st.plotly_chart(fig3, use_container_width=True)

st.divider()

st.subheader("Combustible por unidad producida (mensual)")
df_comb_unit_plot = df_comb_unit.sort_values("mes").copy()
df_comb_unit_plot["galones_por_unidad"] = df_comb_unit_plot["galones_por_unidad"].abs()
fig4 = px.line(
    df_comb_unit_plot,
    x="mes",
    y="galones_por_unidad",
    hover_data=["total_galones", "total_produccion"],
    title="Galones por unidad producida",
)
fig4.update_traces(mode="lines+markers")
fig4.update_layout(height=450, showlegend=False, xaxis_title="Mes")
st.plotly_chart(fig4, use_container_width=True)
