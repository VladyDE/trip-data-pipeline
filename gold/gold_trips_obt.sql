-- =============================================================================
-- gold_viajes_obt.sql
-- One Big Table at trip grain for analytical consumption.
-- Grain: 1 row per trip (id_viaje)
-- =============================================================================
CREATE OR REPLACE MATERIALIZED VIEW {{catalog}}.{{schema}}.gold_viajes_obt
COMMENT 'One big table at trip grain for analytical consumption. Source: driver_satisfaccion_silver.'
AS
SELECT
    -- -------------------------------------------------------------------------
    -- Keys & identifiers
    -- -------------------------------------------------------------------------
    id_viaje,
    placa,

    -- -------------------------------------------------------------------------
    -- Temporal dimensions
    -- -------------------------------------------------------------------------
    fecha                                               AS fecha_ts,
    fecha_solo,
    hora,
    dia_semana,
    semana_anio,
    mes,
    CASE
        WHEN hora BETWEEN 5  AND 11 THEN 'mañana'
        WHEN hora BETWEEN 12 AND 17 THEN 'tarde'
        WHEN hora BETWEEN 18 AND 21 THEN 'noche'
        ELSE                              'madrugada'
    END                                                 AS franja_horaria,
    CASE
        WHEN DAYOFWEEK(fecha_solo) IN (1, 7) THEN 'fin_de_semana'
        ELSE                                      'dia_laboral'
    END                                                 AS tipo_dia,

    -- -------------------------------------------------------------------------
    -- Trip economics
    -- -------------------------------------------------------------------------
    costo_cash,
    estado_desc,
    CASE
        WHEN estado_desc = 'completado' THEN costo_cash
        ELSE 0
    END                                                 AS ingreso_completo,

    -- -------------------------------------------------------------------------
    -- Quality & satisfaction signals
    -- -------------------------------------------------------------------------
    rating_viaje,
    tiene_rating_viaje,
    rating_conductor_al_cliente,
    CASE
        WHEN rating_viaje = 5                           THEN 'excelente'
        WHEN rating_viaje BETWEEN 3 AND 4               THEN 'bueno'
        WHEN rating_viaje BETWEEN 1 AND 2               THEN 'malo'
        ELSE                                                 'sin_rating'
    END                                                 AS categoria_rating,

    -- -------------------------------------------------------------------------
    -- Client
    -- -------------------------------------------------------------------------
    nombre_cliente,
    comentario_cliente,                                 -- array<string> of tags
    SIZE(comentario_cliente)                            AS num_tags_comentario,
    comentario_cliente IS NOT NULL                      AS tiene_comentario_cliente,

    -- -------------------------------------------------------------------------
    -- Origin
    -- -------------------------------------------------------------------------
    origen_manual,
    direccion_cliente

FROM {{catalog}}.{{schema}}.driver_satisfaccion_silver
