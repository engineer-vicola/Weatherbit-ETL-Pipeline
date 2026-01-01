--count of total rows
SELECT COUNT(*) AS total_rows FROM main.weather;

--Temprature and humidity by City
SELECT city,
       ROUND(AVG(temp_c), 2) AS avg_temp_c,
       MIN(temp_c) AS min_temp_c,
       MAX(temp_c) AS max_temp_c,
       ROUND(AVG(rh), 2) AS avg_rh_pct
FROM main.weather
GROUP BY city
ORDER BY avg_temp_c DESC;

--Data quality checks
SELECT _id, COUNT(*) AS duplicates
FROM main.weather
GROUP BY _id
HAVING COUNT(*) > 1;

--Extreme weather alerts
SELECT city, dt, temp_c, uv_index, wind_ms
FROM main.weather
WHERE (uv_index >= 8 OR temp_c >= 35 OR wind_ms >= 15)
ORDER BY dt DESC
LIMIT 50;

