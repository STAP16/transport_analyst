from database.client import get_client

client = get_client()

def create_realtime_traffic():
	query = """
	CREATE TABLE IF NOT EXISTS mart_layer.realtime_traffic (
		camera_id UInt32,
		video_id UInt32,
		timestamp DateTime,
		title String,
		width Float32,
		length Float32,
		total_vehicles UInt32,
		avg_speed_kmh Float32,
		car_count UInt32,
		bus_count UInt32,
		pedestrian_count UInt32,
		truck_count UInt32,
		bicycle_count UInt32,
		motorcycle_count UInt32,
		is_holiday UInt8,
		direction String
	)	
	ENGINE = MergeTree
	ORDER BY (camera_id, timestamp)
	PARTITION BY toYYYYMM(timestamp)

	"""
	client.command(query)

def create_hourly_traffic():
	query = """
	CREATE TABLE IF NOT EXISTS mart_layer.hourly_traffic (
		camera_id UInt32,
		video_id UInt32,
		hour DateTime,
		title String,
		width Float32,
		length Float32,
		total_vehicles UInt32,
		avg_speed_kmh Float32,
		car_count UInt32,
		bus_count UInt32,
		pedestrian_count UInt32,
		truck_count UInt32,
		bicycle_count UInt32,
		motorcycle_count UInt32,
		is_holiday UInt8,
		is_peak_hour UInt8,
		direction String
	)	
	ENGINE = MergeTree
	ORDER BY (camera_id, hour)
	PARTITION BY toYYYYMM(hour)

	"""
	client.command(query)

def create_system_apperances():
	query = """
	CREATE TABLE IF NOT EXISTS mart_layer.system_apperances (
		camera_id UInt32,
		video_id UInt32,
		timestamp DateTime,
		title String,
		width Float32,
		length Float32,
		total_vehicles UInt32,
		avg_speed_kmh Float32,
		car_count UInt32,
		bus_count UInt32,
		truck_count UInt32,
		is_overdrawing UInt8,
		is_holiday UInt8,
		is_peak_hour UInt8,
		direction String
	)	
	ENGINE = MergeTree
	ORDER BY (camera_id, timestamp)
	PARTITION BY toYYYYMM(timestamp)

	"""
	client.command(query)	


def create_proximity_events():
	query = """
	CREATE TABLE IF NOT EXISTS mart_layer.system_apperances (
		camera_id UInt32,
		video_id UInt32,
		timestamp DateTime,
		first_track_id UInt32,
		second_track_id UInt32,
		first_vehicle_type String,
		second_vehicle_type String,
		distanse_m Float32,
		is_danger_proximity UInt8,
		direction String
	)	
	ENGINE = MergeTree
	ORDER BY (camera_id, timestamp)
	PARTITION BY toYYYYMM(timestamp)

	"""
	client.command(query)	

def create_dispatcher_predictions():
	query = """
	CREATE TABLE IF NOT EXISTS mart_layer.dispatcher_predictions (
		camera_id UInt32,
		video_id UInt32,
		predicted_at DateTime,
		width Float32,
		length Float32,
		total_vehicles UInt32,
		avg_speed_kmh30 Float32,
		avg_speed_kmh60 Float32,
		avg_speed_kmh120 Float32,
		is_traffic_jam30 UInt8,
		is_traffic_jam60 UInt8,
		is_traffic_jam120 UInt8,
		is_overdrawing UInt8,
		is_holiday UInt8,
		is_peak_hour UInt8,
		recomendation String,
		expected_result String,
		direction String
	)	
	ENGINE = MergeTree
	ORDER BY (camera_id, predicted_at)
	PARTITION BY toYYYYMM(predicted_at)

	"""
	client.command(query)	

