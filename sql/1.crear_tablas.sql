 
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
where
a.unidade = 'HA'
group by 
  a.func, 
  a.empresa, 
  a.actividad ;
  
CREATE OR REPLACE VIEW stage.vista_costo_insumos_por_hectarea
AS SELECT i.zona,
    i.nm_faz AS hacienda,
    i.nm_actividad AS actividad,
    to_date(i.fecha, 'dd/mm/yyyy'::text) AS fecha,
    sum(i.valor) AS costo_total,
    sum(i.area_apli) AS area_total,
        CASE
            WHEN sum(i.area_apli) > 0::numeric THEN round(sum(i.valor) / sum(i.area_apli), 2)
            ELSE 0::numeric
        END AS costo_por_hectarea
   FROM raw.insumos i
  where um_prod = 'HA'
  GROUP BY i.zona, i.nm_faz, i.nm_actividad, i.fecha;
 
 
 DROP VIEW IF EXISTS stage.vista_produccion_maquinaria;

 CREATE 
OR REPLACE VIEW stage.vista_produccion_maquinaria AS WITH produccion AS (
  SELECT 
    --rm.equipo,
    rm.nombre_actividad, 
    --TO_DATE(rm.fecha, 'DD/MM/YYYY') AS fecha,
    --rm.empresa_de_la_maquina,
    SUM(rm.cantidad_produccion) AS total_produccion, 
    SUM(rm.duracion_horas) AS total_horas, 
    CASE WHEN SUM(rm.duracion_horas) > 0 THEN ROUND(
      SUM(rm.cantidad_produccion) / SUM(rm.duracion_horas), 
      2
    ) ELSE 0 END AS produccion_por_hora 
  FROM 
    raw.rep_maquinaria rm 
  WHERE 
    rm.unidad_produccion = 'HA' 
  GROUP BY 
    --rm.equipo, 
    rm.nombre_actividad --rm.fecha, 
    --rm.empresa_de_la_maquina
    ) 
SELECT 
  * 
FROM 
  produccion 
ORDER BY 
  produccion_por_hora DESC 
LIMIT 
  5;
 
 drop view if exists stage.vista_combustible_por_unidad_producida;
 
 CREATE 
OR REPLACE VIEW stage.vista_combustible_por_unidad_producida AS 
SELECT 
  --a.equipo, 
  to_char(
    to_date(a.fecha, 'dd/mm/yyyy' :: text):: timestamp with time zone, 
    'yyyy-mm' :: text
  ) AS mes, 
  sum(a.galones) AS total_galones, 
  sum(rm.cantidad_produccion) AS total_produccion, 
  CASE WHEN sum(rm.cantidad_produccion) > 0 :: numeric THEN round(
    sum(a.galones) / sum(rm.cantidad_produccion), 
    2
  ) ELSE 0 :: numeric END AS galones_por_unidad 
FROM 
  raw.abastecimientos a 
  JOIN raw.rep_maquinaria rm ON a.equipo = rm.equipo 
  AND to_date(a.fecha, 'dd/mm/yyyy' :: text) = to_date(rm.fecha, 'dd/mm/yyyy' :: text) 
where 
  rm.unidad = 'H' 
GROUP BY 
  --a.equipo, 
  (
    to_char(
      to_date(a.fecha, 'dd/mm/yyyy' :: text):: timestamp with time zone, 
      'yyyy-mm' :: text
    )
  );

COMMIT;
