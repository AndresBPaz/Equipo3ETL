 
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

COMMIT;