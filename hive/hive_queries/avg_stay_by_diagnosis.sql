-- Calculates average length of stay per diagnosis

SELECT
  diagnosis,
  ROUND(AVG(stay_length), 2) AS avg_stay_days,
  COUNT(*) AS total_admissions
FROM admissions
GROUP BY diagnosis
ORDER BY avg_stay_days DESC
LIMIT 20;