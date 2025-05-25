-- Identifies patients with more than one ICU stay

SELECT
  subject_id,
  COUNT(*) AS icu_visits
FROM icustays
GROUP BY subject_id
HAVING COUNT(*) > 1
ORDER BY icu_visits DESC
LIMIT 20;