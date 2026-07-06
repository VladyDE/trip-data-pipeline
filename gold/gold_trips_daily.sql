-- =============================================================================
-- Aggregated daily metrics for analytical consumption.
-- Grain: 1 row per day (fecha_solo)
-- =============================================================================
USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

CREATE OR REPLACE TABLE daily_metrics_gold
COMMENT 'Daily metrics aggregated.'
AS
SELECT
    -- -------------------------------------------------------------------------
    -- Keys & temporal dimensions
    -- -------------------------------------------------------------------------
    fecha_solo,
    FIRST(dia_semana)                                              AS dia_semana,
    FIRST(semana_anio)                                             AS semana_anio,
    FIRST(mes)                                                     AS mes,
    CASE
        WHEN DAYOFWEEK(fecha_solo) IN (1, 7) THEN 'fin_de_semana'
        ELSE                                      'dia_laboral'
    END                                                             AS tipo_dia,

    -- -------------------------------------------------------------------------
    -- Volume
    -- -------------------------------------------------------------------------
    COUNT(*)                                                       AS total_viajes,
    COUNT(DISTINCT placa)                                          AS conductores_activos,

    -- -------------------------------------------------------------------------
    -- Estado / cancelaciones
    -- -------------------------------------------------------------------------
    SUM(CASE WHEN estado_desc = 'completado' THEN 1 ELSE 0 END)    AS viajes_completados,
    SUM(CASE WHEN estado_desc = 'cancelado'  THEN 1 ELSE 0 END)    AS viajes_cancelados,
    SUM(CASE WHEN estado_desc = 'en_proceso' THEN 1 ELSE 0 END)    AS viajes_en_proceso,
    ROUND(
        SUM(CASE WHEN estado_desc = 'cancelado' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0),
        2
    )                                                               AS pct_cancelados,

    -- -------------------------------------------------------------------------
    -- Ingresos
    -- -------------------------------------------------------------------------
    ROUND(SUM(CASE WHEN estado_desc = 'completado' THEN costo_cash ELSE 0 END), 2)
                                                                    AS ingresos_totales,
    ROUND(AVG(CASE WHEN estado_desc = 'completado' THEN costo_cash END), 2)
                                                                    AS ingreso_promedio_por_viaje,

    -- -------------------------------------------------------------------------
    -- Rating / satisfacción
    -- -------------------------------------------------------------------------
    ROUND(AVG(rating_viaje), 2)                                    AS rating_promedio,
    SUM(CASE WHEN tiene_rating_viaje THEN 1 ELSE 0 END)            AS viajes_con_rating,
    ROUND(
        SUM(CASE WHEN tiene_rating_viaje THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0),
        2
    )                                                               AS pct_viajes_con_rating,
    SUM(CASE WHEN rating_viaje = 5 THEN 1 ELSE 0 END)              AS viajes_rating_excelente,
    SUM(CASE WHEN rating_viaje BETWEEN 1 AND 2 THEN 1 ELSE 0 END)  AS viajes_rating_malo,
    ROUND(AVG(rating_conductor_al_cliente), 2)                     AS rating_conductor_promedio

FROM driver_satisfaccion_silver
GROUP BY fecha_solo