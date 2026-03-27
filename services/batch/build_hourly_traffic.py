def build_hourly_traffic():
	from database.client import get_client
	client = get_client()

	query = """
	INSERT INTO mart_layer.hourly_traffic
	(
		camera_id,
		video_id,
		hour,
		title,
		width,
		length,
		total_vehicles,
		avg_speed_kmh,
		car_count,
		bus_count,
		pedestrian_count,
		truck_count,
		bicycle_count,
		motorcycle_count,
		is_holiday,
		is_peak_hour,
		direction
	)
	SELECT 
		camera_id,
		camera_id AS video_id,
		toStartOfHour(detection_time) AS hour,
		title,
		avg(width) AS width,
		avg(length) AS length,
		uniq(track_id) AS total_vehicles,
		avg(speed_km_h) AS avg_speed_kmh,
		uniqIf(track_id, object_type = 'car') AS car_count,
		uniqIf(track_id, object_type = 'bus') AS bus_count,
		uniqIf(track_id, object_type = 'pedestrian') AS pedestrian_count,
		uniqIf(track_id, object_type = 'truck') AS truck_count,
		uniqIf(track_id, object_type = 'bicycle') AS bicycle_count,
		uniqIf(track_id, object_type = 'motorcycle') AS motorcycle_count,
		0 AS is_holiday,
		toHour(toStartOfHour(detection_time)) IN (7, 8, 9, 17, 18, 19) AS is_peak_hour,
		any(direction) AS direction
	FROM staging_layer.detections_clean
	GROUP BY toStartOfHour(detection_time), title, camera_id
	"""

	client.command(query)
	print("Hourly traffic has been inserted")

build_hourly_traffic()