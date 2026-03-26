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