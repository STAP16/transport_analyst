from database.client import get_client

client = get_client()

def create_detections_clean_table():
	query = """
	CREATE TABLE IF NOT EXISTS staging_layer.detections_clean (
		camera_id UInt32,
		video_id UInt32,
		title String,
		recording_at DateTime,
		recording_end DateTime,
		track_id UInt32,
		start_time DateTime64(3),
		end_time DateTime64(3),
		object_type String,
		width Float32,
		length Float32,
		detection_time DateTime64(3),
		x_cord_m Float32,
		y_cord_m Float32,
		total_kms Float32,
		speed_km_h Float32,
		direction Float32
	)	
	ENGINE = MergeTree
	ORDER BY (camera_id, detection_time)
	PARTITION BY toYYYYMM(detection_time)

	"""
	client.command(query)