-- KPI 1: Average prediction error
SELECT ROUND(AVG(ABS(prediction_error))::NUMERIC, 4) AS avg_prediction_error
FROM fact_predictions;

-- KPI 2: Predictions by country
SELECT c.country_name,
       COUNT(*) AS total_predictions,
       ROUND(AVG(predicted_score)::NUMERIC, 3) AS avg_predicted_score
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
GROUP BY c.country_name
ORDER BY total_predictions DESC;

-- KPI 3: Predicted vs Actual score
SELECT c.country_name, d.year,
       f.actual_score, f.predicted_score,
       ROUND(ABS(f.prediction_error)::NUMERIC, 4) AS abs_error
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
JOIN dim_date d ON f.date_id = d.date_id
ORDER BY d.year, c.country_name;

-- KPI 4: Prediction trends over time
SELECT d.year,
       COUNT(*) AS total_predictions,
       ROUND(AVG(f.predicted_score)::NUMERIC, 3) AS avg_predicted,
       ROUND(AVG(f.actual_score)::NUMERIC, 3) AS avg_actual,
       ROUND(AVG(ABS(f.prediction_error))::NUMERIC, 4) AS avg_error
FROM fact_predictions f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;
