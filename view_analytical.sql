CREATE OR REPLACE VIEW `niktrack-project.chicago_taxi_trip_data_project.view_analytical` AS (
SELECT 
tf.taxi_trip_id,
dd.trip_start_timestamp,
dd.trip_end_timestamp,
td.trip_miles,
td.trip_km,
pt.payment_type,
th.trip_seconds,
th.trip_hour,
cp.company_name,
dca.dropoff_community_area,
pca.pickup_community_area,
ppl.pickup_centroid_latitude,
ppl.pickup_centroid_longitude,
dpl.dropoff_centroid_latitude,
dpl.dropoff_centroid_longitude,
tf.fare,
tf.tips,
tf.tolls,
tf.extras,
tf.trip_total

FROM 
`niktrack-project.chicago_taxi_trip_data_project.taxi_trip_fact` tf
JOIN `niktrack-project.chicago_taxi_trip_data_project.datetime_dim` dd  ON tf.datetime_id=dd.datetime_id
JOIN `niktrack-project.chicago_taxi_trip_data_project.trip_distance_dim` td  ON td.trip_distance_id=tf.trip_distance_id
JOIN `niktrack-project.chicago_taxi_trip_data_project.payment_type_dim` pt ON pt.payment_type_id=tf.payment_type_id
JOIN `niktrack-project.chicago_taxi_trip_data_project.trip_hour_dim` th ON th.trip_hour_id=tf.trip_hour_id  
JOIN `niktrack-project.chicago_taxi_trip_data_project.company_dim` cp ON cp.company_id=tf.company_id
JOIN `niktrack-project.chicago_taxi_trip_data_project.dropoff_comunity_area_dim` dca ON dca.dropoff_community_area_id=tf.dropoff_community_area_id 
JOIN `niktrack-project.chicago_taxi_trip_data_project.pickup_comunity_area_dim` pca ON pca.pickup_community_area_id=tf.pickup_community_area_id
JOIN `niktrack-project.chicago_taxi_trip_data_project.dropoff_location_dim` dpl ON dpl.dropoff_location_id=tf.dropoff_location_id
JOIN `niktrack-project.chicago_taxi_trip_data_project.pickup_location_dim` ppl ON ppl.pickup_location_id=tf.pickup_location_id);