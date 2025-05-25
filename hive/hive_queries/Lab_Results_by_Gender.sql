SELECT
    p.gender,
    l.itemid,
    AVG(l.valuenum) AS avg_value,
    COUNT(*) AS test_count
FROM patients p
JOIN admissions a ON p.subject_id = a.subject_id
JOIN labevents l ON a.hadm_id = l.hadm_id
WHERE l.valuenum IS NOT NULL
GROUP BY p.gender, l.itemid
ORDER BY test_count DESC
LIMIT 5;