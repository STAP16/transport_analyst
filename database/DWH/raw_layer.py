from database.client import get_client

client = get_client()

def create_raw_detections_table():
	query = """
	CREATE TABLE IF NOT EXISTS raw_layer.raw_detections (
		camera_id UInt32,
		video_id UInt32,
		title String,
		track_id UInt32,
		object_type String,
		width Float32,
		length Float32,
		detection_time DateTime64(3),
		x_cord_m Float32,
		y_cord_m Float32,
		direction Float32,
		confidence Float32
	)	
	ENGINE = MergeTree
	ORDER BY (camera_id, detection_time)
	PARTITION BY toYYYYMM(detection_time)

	"""
	client.command(query)


def create_csv_traffic_raw():
	query = """
	CREATE TABLE IF NOT EXISTS raw_layer.csv_traffic_raw (
		video_id UInt32,
		title String,
		path String,
		recording_at DateTime64(6),
		recording_end Nullable(DateTime64(6)),
		track_id UInt32,
		tracker_id UInt32,
		start_time DateTime64(6),
		end_time Nullable(DateTime64(6)),
		object_type String,
		width Float32,
		length Float32,
		detection_time DateTime64(6),
		x_cord_m Float32,
		y_cord_m Float32,
		total_kms Nullable(Float32),
		speed_km_h Nullable(Float32),
		direction Nullable(String),
		loaded_at DEFAULT now()
	)	
	ENGINE = MergeTree
	ORDER BY (tracker_id, detection_time)
	PARTITION BY toYYYYMM(detection_time)

	"""
	client.command(query)