-- Join with admissions to analyze outcomes:

SELECT
    a.admission_type,
    c.callout_outcome,
    COUNT(*) AS callout_count
FROM admissions a
JOIN callout c ON a.hadm_id = c.hadm_id
GROUP BY a.admission_type, c.callout_outcome
ORDER BY callout_count DESC;
