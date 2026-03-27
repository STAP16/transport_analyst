def transform_raw_csv_to_staging():

	import numpy as np
	from database.client import get_client

	client = get_client()
	DEFAULT_DIRECTION = "north"

	query = """
		SELECT * FROM raw_layer.csv_traffic_raw
		ORDER BY track_id, detection_time
	"""

	def clean_object_type(type):
		match type:
			case "medium car":
				return "car"
			case "light car":
				return "car"
			case _:
				return type

	def calculate_speed(first_track_point, last_track_point):
		dist = np.sqrt(
			(last_track_point["x_cord_m"] - first_track_point["x_cord_m"])**2 +
			(last_track_point["y_cord_m"] - first_track_point["y_cord_m"])**2
		)

		time_diff = (last_track_point["detection_time"] - first_track_point["detection_time"]).dt.total_seconds()

		speed_km_h = (dist/time_diff) * 3.6
		total_kms = dist / 1000
		return speed_km_h, total_kms

	df = client.query_df(query)
	df["object_type"] = df["object_type"].apply(clean_object_type)
	df = df.rename(columns={'tracker_id': 'object_id'})

	first_track_point = df.groupby("track_id").first()
	last_track_point = df.groupby("track_id").last()
	speed_km_h, total_kms = calculate_speed(first_track_point, last_track_point)
	df['camera_id'] = df['video_id']
	df["speed_km_h"] = df["track_id"].map(speed_km_h).fillna(0)
	df["total_kms"] = df["track_id"].map(total_kms).fillna(0)
	df["direction"] = df["direction"].fillna(DEFAULT_DIRECTION)

	columns = [
		'camera_id',
		'video_id',
		'title',
		'recording_at',
		'recording_end',
		'track_id',
		'start_time',
		'end_time',
		'object_type',
		'width',
		'length',
		'detection_time',
		'x_cord_m',
		'y_cord_m',
		'total_kms',
		'speed_km_h',
		'direction'
	]

	client.insert_df("staging_layer.detections_clean", df[columns])
	print("Data from raw_csv succesfull loaded at staging layer")


transform_raw_csv_to_staging()