## Example 1: Total Admissions per Diagnosis :


SELECT diagnosis, COUNT(*) AS total_admissions

FROM admissions

GROUP BY diagnosis

ORDER BY total_admissions DESC

LIMIT 10;

## Example 2: Average Age by Gender :
SELECT gender, AVG(age) AS avg_age

FROM patients

GROUP BY gender;
## Example 3: Average Length of Stay per Admission Type :
SELECT admission_type, AVG(stay_length) AS avg_days

FROM admissions

GROUP BY admission_type;
## Example 4: Repeated ICU Visits
SELECT subject_id, COUNT(*) AS icu_visits 

FROM icustays

GROUP BY subject_id

HAVING COUNT(*) > 1;


