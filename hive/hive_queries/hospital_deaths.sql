-- Analyze if callouts correlate with hospital deaths:

SELECT
    c.callout_service,
    COUNT(*) AS total_callouts,
    SUM(CASE WHEN a.hospital_expire_flag THEN 1 ELSE 0 END) AS deaths,
    (SUM(CASE WHEN a.hospital_expire_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS mortality_rate
FROM callout c
JOIN admissions a ON c.hadm_id = a.hadm_id
GROUP BY c.callout_service
ORDER BY mortality_rate DESC;