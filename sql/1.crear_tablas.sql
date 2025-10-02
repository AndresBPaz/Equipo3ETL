 
-- Crear schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS analytics;

DROP VIEW IF EXISTS stage.vista_limpia;
DROP MATERIALIZED VIEW IF EXISTS analytics.reporte_final ; 

-- Tablas en esquema raw
DROP TABLE IF EXISTS raw.abastecimientos;
CREATE TABLE IF NOT EXISTS raw.abastecimientos (
  id TEXT,
  -- empresa TEXT,
  -- producto TEXT,
  -- cantidad NUMERIC,
  -- unidad TEXT,
  -- valor NUMERIC,
  -- observaciones TEXT
  equipo TEXT,
  fecha TEXT,
  galones NUMERIC

);

DROP TABLE IF EXISTS raw.actividades;
CREATE TABLE IF NOT EXISTS raw.actividades (
  id TEXT,
  empresa TEXT,
  fazenda TEXT,
  lote TEXT,
  talhao TEXT,
  ccusto TEXT,
  cencos TEXT,
  oper TEXT,
  actividad TEXT,
  data TEXT,
  quantidade NUMERIC,
  a_pag_uni NUMERIC,
  valor_total NUMERIC,
  unidade TEXT,
  um_prod TEXT,
  doc_erp1 TEXT,
  os TEXT,
  func TEXT,
  qtd_prod NUMERIC,
  area_real_suerte NUMERIC
);

DROP TABLE IF EXISTS raw.insumos;
CREATE TABLE IF NOT EXISTS raw.insumos (
  id TEXT,
  hda TEXT,
  zona TEXT,
  fecha TEXT,
  nm_faz TEXT,
  lote TEXT,
  tal TEXT,
  ocup TEXT,
  os TEXT,
  nm_actividad TEXT,
  ccusto TEXT,
  cenco_nom TEXT,
  empresa TEXT,
  produto TEXT,
  um TEXT,
  nm_producto TEXT,
  nm_tp_prod TEXT,
  um_prod TEXT,
  dosis NUMERIC,
  area_apli NUMERIC,
  area_real NUMERIC,
  area NUMERIC,
  cant_apli NUMERIC,
  valor NUMERIC
);

DROP TABLE IF EXISTS raw.rep_maquinaria;
CREATE TABLE IF NOT EXISTS raw.rep_maquinaria (
  id TEXT,
  codigo_equipo TEXT,
  equipo TEXT,
  ccusto_act TEXT,
  actividad TEXT,
  nombre_actividad TEXT,
  fecha TEXT,
  implemento_1 TEXT,
  hora_inicial TIME,
  hora_final TIME,
  duracion_horas NUMERIC,
  duracion_medidor NUMERIC,
  empresa_de_la_maquina TEXT,
  unidad_produccion TEXT,
  cantidad_produccion NUMERIC,
  horometro_inicial NUMERIC,
  horometro_final NUMERIC,
  unidad TEXT,
  cantidad NUMERIC,
  hda TEXT,
  lote TEXT,
  tal TEXT,
  nro_de_la_os TEXT
);

-- Vista de ejemplo (usando abastecimientos en vez de tabla ficticia)
CREATE OR REPLACE VIEW stage.vista_limpia AS
SELECT *
FROM raw.abastecimientos ;

-- Materialized view de ejemplo (agrupando por producto)
-- CREATE MATERIALIZED VIEW analytics.reporte_final AS
-- SELECT producto, AVG(valor) AS promedio_valor
-- FROM stage.vista_limpia
-- GROUP BY producto;

-- Vista de consumo realizado por los equipos por mes
create or replace view stage.vista_consumo_equipo_mensual as 
select 
  a.equipo, 
  to_char(
    to_date(a.fecha, 'dd/mm/yyyy'), 
    'yyyy-mm'
  ) as mes, 
  sum(a.galones) as total_galones, 
  avg(a.galones) as promedio_galones 
from 
  raw.abastecimientos a 
group by 
  a.equipo, 
  to_char(
    to_date(a.fecha, 'dd/mm/yyyy'), 
    'yyyy-mm'
  ) ;

-- Vista de costos por actividad
create or replace view stage.vista_costo_actividad as 
select 
  a.empresa, 
  a.fazenda, 
  a.lote, 
  a.actividad, 
  to_date(a."data", 'dd/mm/yyyy') as fecha, 
  sum(a.a_pag_uni * a.quantidade) as costo_total_actividad 
from 
  raw.actividades a 
group by 
  a.empresa, 
  a.fazenda, 
  a.lote, 
  a.actividad, 
  a."data" ;

-- Vista de productividad del trabajador
create or replace view stage.vista_productividad_trabajador as 
select 
  a.func as trabajador, 
  a.empresa, 
  a.actividad, 
  sum(a.qtd_prod) as total_producido, 
  count(*) as cantidad_registros, 
  avg(a.qtd_prod) as promedio_produccion 
from 
  raw.actividades a 
group by 
  a.func, 
  a.empresa, 
  a.actividad ;

-- Vista de costo insumo por hetarea
create or replace view stage.vista_costo_insumos_por_hectarea as 
select 
  i.zona, 
  i.nm_faz as hacienda, 
  i.nm_actividad as actividad, 
  to_date(i.fecha, 'dd/mm/yyyy') as fecha, 
  sum(i.valor) as costo_total, 
  sum(i.area_apli) as area_total, 
  case when sum(i.area_apli) > 0 then round(
    sum(i.valor):: numeric / sum(i.area_apli), 
    2
  ) else 0 end as costo_por_hectarea 
from 
  raw.insumos i 
group by 
  i.zona, 
  i.nm_faz, 
  i.nm_actividad, 
  i.fecha ;

-- Vista de producción por maquinaria
create or replace view stage.vista_produccion_maquinaria as 
select 
  rm.equipo, 
  rm.nombre_actividad, 
  to_date(rm.fecha, 'dd/mm/yyyy') as fecha, 
  rm.empresa_de_la_maquina, 
  sum(rm.cantidad_produccion) as total_produccion, 
  sum(rm.duracion_horas) as total_horas, 
  case when sum(rm.duracion_horas) > 0 then round(
    sum(rm.cantidad_produccion):: numeric / sum(rm.duracion_horas), 
    2
  ) else 0 end as produccion_por_hora 
from 
  raw.rep_maquinaria rm 
group by 
  rm.equipo, 
  rm.nombre_actividad, 
  rm.fecha, 
  rm.empresa_de_la_maquina ;

-- Vista de combustible por unidad producida
create 
or replace view stage.vista_combustible_por_unidad_producida as 
select 
  a.equipo, 
  to_char(
    to_date(a.fecha, 'dd/mm/yyyy'), 
    'yyyy-mm'
  ) as mes, 
  sum(a.galones) as total_galones, 
  sum(rm.cantidad_produccion) as total_produccion, 
  case when sum(rm.cantidad_produccion) > 0 then round(
    sum(a.galones):: numeric / sum(rm.cantidad_produccion), 
    2
  ) else 0 end as galones_por_unidad 
from 
  raw.abastecimientos a 
  join raw.rep_maquinaria rm on a.equipo = rm.equipo 
  and to_date(a.fecha, 'dd/mm/yyyy') = to_date(rm.fecha, 'dd/mm/yyyy') 
group by 
  a.equipo, 
  to_char(
    to_date(a.fecha, 'dd/mm/yyyy'), 
    'yyyy-mm'
  ) ;

COMMIT;
