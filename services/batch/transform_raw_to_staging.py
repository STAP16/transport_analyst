def transform_raw_to_staging():
	from database.client import get_client
	import pandas as pd

	from services.batch.utils.calculate_speed import calculate_speed
	client = get_client()

	query = "SELECT * FROM raw_layer.raw_detections WHERE confidence > 0.5"
	df = client.query_df(query)


	def angle_to_direction(angle):
		if angle is None or pd.isna(angle):
				return 'UNKNOWN'
		angle = float(angle) % 360
		if angle < 45 or angle >= 315:
				return 'north'
		elif angle < 135:
				return 'east'
		elif angle < 225:
				return 'south'
		else:
				return 'west'


	first_track_point = df.groupby("track_id").first()
	last_track_point = df.groupby("track_id").last()
	speed_km_h, total_kms = calculate_speed(first_track_point, last_track_point)
	df['camera_id'] = df['video_id']
	df["speed_km_h"] = df["track_id"].map(speed_km_h).fillna(0)
	df["total_kms"] = df["track_id"].map(total_kms).fillna(0)
	df['direction'] = df['direction'].apply(angle_to_direction)
	last_time = last_track_point["detection_time"]
	df['recording_at'] = df["detection_time"]
	df['recording_end'] = df['track_id'].map(last_time)
	df['start_time'] = df["detection_time"]
	df['end_time'] = df['track_id'].map(last_time)

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
	print("Data from raw_detections succesfull loaded at staging layer")


transform_raw_to_staging()