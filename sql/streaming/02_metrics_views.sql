CREATE OR REPLACE VIEW streaming.v_metricas_pipeline AS
SELECT
    COUNT(*) AS total_eventos,
    COUNT(*) FILTER (WHERE processing_status = 'processed') AS eventos_procesados,
    COUNT(*) FILTER (WHERE processing_status = 'rejected')  AS eventos_rechazados,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE processing_status = 'rejected')
        / NULLIF(COUNT(*), 0), 2
    ) AS tasa_rechazo_pct,
    MAX(processed_at) AS ultimo_procesamiento
FROM streaming.raw_events;

CREATE OR REPLACE VIEW streaming.v_eventos_por_minuto AS
SELECT
    date_trunc('minute', COALESCE(processed_at, received_at)) AS minuto,
    processing_status,
    COUNT(*) AS total_eventos
FROM streaming.raw_events
GROUP BY 1, 2;

CREATE OR REPLACE VIEW streaming.v_avance_simulacion AS
SELECT
    f.simulation_id,
    COUNT(*) AS eventos_procesados,
    COUNT(DISTINCT f.sk_fecha) AS jornadas_procesadas,
    COUNT(DISTINCT f.sk_geografia) AS municipios_reportados,
    (SELECT COUNT(*) FROM comun.dim_geografia
      WHERE codigo_departamento = '76') AS municipios_esperados,
    MIN(d.fecha) AS fecha_inicial,
    MAX(d.fecha) AS fecha_final,
    MIN(f.published_at) AS primera_publicacion,
    MAX(f.loaded_at) AS ultima_carga,
    ROUND(AVG(EXTRACT(EPOCH FROM (f.loaded_at - f.published_at))), 2)
        AS latencia_promedio_seg
FROM streaming.fact_weather_daily f
JOIN weather.dim_fecha d ON d.sk_fecha = f.sk_fecha
GROUP BY f.simulation_id;

CREATE OR REPLACE VIEW streaming.v_clima_por_jornada AS
SELECT
    f.simulation_id,
    d.fecha,
    COUNT(*) AS municipios_reportados,
    ROUND(AVG(f.temperature_mean), 2) AS temperatura_media,
    ROUND(MAX(f.temperature_max), 2) AS temperatura_maxima,
    ROUND(MIN(f.temperature_min), 2) AS temperatura_minima,
    ROUND(SUM(f.precipitation_mm), 2) AS precipitacion_total_mm,
    ROUND(AVG(f.relative_humidity), 2) AS humedad_promedio,
    ROUND(AVG(f.et0_mm), 2) AS et0_promedio_mm,
    MAX(f.loaded_at) AS ultima_carga
FROM streaming.fact_weather_daily f
JOIN weather.dim_fecha d ON d.sk_fecha = f.sk_fecha
GROUP BY f.simulation_id, d.fecha;

CREATE OR REPLACE VIEW streaming.v_clima_por_municipio AS
SELECT
    f.simulation_id,
    g.codigo_municipio,
    g.municipio,
    COUNT(*) AS jornadas,
    ROUND(AVG(f.temperature_mean), 2) AS temperatura_media,
    ROUND(SUM(f.precipitation_mm), 2) AS precipitacion_total_mm,
    ROUND(AVG(f.relative_humidity), 2) AS humedad_promedio
FROM streaming.fact_weather_daily f
JOIN comun.dim_geografia g ON g.sk_geografia = f.sk_geografia
GROUP BY f.simulation_id, g.codigo_municipio, g.municipio;

CREATE OR REPLACE VIEW streaming.v_eventos_rechazados AS
SELECT
    raw_event_id,
    event_id,
    parsed_payload->>'simulation_id' AS simulation_id,
    parsed_payload->>'municipality_code' AS codigo_municipio,
    parsed_payload->>'relative_humidity' AS humedad_recibida,
    kafka_partition,
    kafka_offset,
    error_message,
    processed_at
FROM streaming.raw_events
WHERE processing_status = 'rejected';