-- Total Trips
SELECT COUNT(*) FROM trips_schema.trips;

-- Average Fare
SELECT AVG(fare) FROM trips_schema.trips;

-- Daily Trips
SELECT DATE(pickup_time), COUNT(*)
FROM trips_schema.trips
GROUP BY DATE(pickup_time);

-- Highest Fare Trips
SELECT *
FROM trips_schema.trips
ORDER BY fare DESC
LIMIT 5;

-- INNER JOIN
SELECT t.trip_id,
       d.driver_name,
       t.fare
FROM trips_schema.trips t
JOIN trips_schema.drivers d
ON t.driver_id = d.driver_id;

-- LEFT JOIN
SELECT t.trip_id,
       d.driver_name
FROM trips_schema.trips t
LEFT JOIN trips_schema.drivers d
ON t.driver_id = d.driver_id;

-- Window Function
SELECT trip_id,
       fare,
       RANK() OVER (ORDER BY fare DESC)
FROM trips_schema.trips;

-- CTE Example
WITH avg_fare AS (
   SELECT AVG(fare) avg_val
   FROM trips_schema.trips
)
SELECT *
FROM trips_schema.trips
WHERE fare > (SELECT avg_val FROM avg_fare);

