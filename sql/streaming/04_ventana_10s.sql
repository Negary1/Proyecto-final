CREATE OR REPLACE VIEW streaming.v_eventos_por_minuto AS
SELECT
    date_bin('10 seconds',
        COALESCE(processed_at, received_at),
        TIMESTAMPTZ '2000-01-01 00:00:00+00'
    ) AT TIME ZONE 'America/Bogota' AS minuto,
    processing_status,
    COUNT(*) AS total_eventos
FROM streaming.raw_events
GROUP BY 1, 2;