DROP VIEW IF EXISTS streaming.v_metricas_pipeline;
DROP VIEW IF EXISTS streaming.v_eventos_por_minuto;
DROP VIEW IF EXISTS streaming.v_eventos_rechazados;
DROP VIEW IF EXISTS streaming.v_simulaciones;

CREATE VIEW streaming.v_simulaciones AS
SELECT DISTINCT simulation_id
FROM streaming.fact_weather_daily;

CREATE VIEW streaming.v_metricas_pipeline AS
SELECT
    COUNT(*) AS total_eventos,
    COUNT(*) FILTER (WHERE processing_status = 'processed') AS eventos_procesados,
    COUNT(*) FILTER (WHERE processing_status = 'rejected')  AS eventos_rechazados,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE processing_status = 'rejected')
        / NULLIF(COUNT(*), 0), 2
    ) AS tasa_rechazo_pct,
    (MAX(processed_at) AT TIME ZONE 'America/Bogota') AS ultimo_procesamiento
FROM streaming.raw_events;

CREATE VIEW streaming.v_eventos_por_minuto AS
SELECT
    date_trunc('minute',
        COALESCE(processed_at, received_at) AT TIME ZONE 'America/Bogota'
    ) AS minuto,
    processing_status,
    COUNT(*) AS total_eventos
FROM streaming.raw_events
GROUP BY 1, 2;

CREATE VIEW streaming.v_eventos_rechazados AS
SELECT
    raw_event_id,
    event_id,
    parsed_payload->>'simulation_id' AS simulation_id,
    parsed_payload->>'municipality_code' AS codigo_municipio,
    parsed_payload->>'relative_humidity' AS humedad_recibida,
    kafka_partition,
    kafka_offset,
    error_message,
    (processed_at AT TIME ZONE 'America/Bogota') AS procesado_hora_colombia
FROM streaming.raw_events
WHERE processing_status = 'rejected';