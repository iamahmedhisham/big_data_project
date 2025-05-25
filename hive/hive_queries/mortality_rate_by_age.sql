-- Computes death counts and rates by age group

SELECT
  CASE
    WHEN age < 30 THEN '<30'
    WHEN age BETWEEN 30 AND 60 THEN '30-60'
    ELSE '>60'
  END AS age_group,
  COUNT(*) AS total_patients,
  SUM(CASE WHEN dod IS NOT NULL THEN 1 ELSE 0 END) AS deaths,
  ROUND(100.0 * SUM(CASE WHEN dod IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS mortality_rate
FROM patients
GROUP BY
  CASE
    WHEN age < 30 THEN '<30'
    WHEN age BETWEEN 30 AND 60 THEN '30-60'
    ELSE '>60'
  END
ORDER BY mortality_rate DESC;