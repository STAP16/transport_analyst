def build_realtime_traffic():
	from database.client import get_client
	client = get_client()

	query = """
	INSERT INTO mart_layer.realtime_traffic
	(
		camera_id,
		video_id,
		timestamp,
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
		direction
	)
	SELECT 
		camera_id,
		camera_id AS video_id,
		toStartOfFiveMinutes(detection_time) AS timestamp,
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
		any(direction) AS direction
	FROM staging_layer.detections_clean
	GROUP BY toStartOfFiveMinutes(detection_time), title, camera_id
	"""

	client.command(query)
	print("Realtime metrics has been inserted")

build_realtime_traffic()