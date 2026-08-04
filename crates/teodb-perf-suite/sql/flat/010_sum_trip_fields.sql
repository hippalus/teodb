SELECT
  SUM(passenger_count) AS passenger_count_sum,
  SUM(trip_distance) AS trip_distance_sum,
  SUM(fare_amount) AS fare_amount_sum
FROM default.nyc_taxi;
