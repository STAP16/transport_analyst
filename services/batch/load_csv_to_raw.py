

def load_csv_to_raw(csv_path: str):
	import pandas as pd
	from database.client import get_client
	client = get_client()

	df = pd.read_csv(csv_path)

	date_columns = [
		'recording_at',
		'recording_end',
		'start_time',
		'end_time',
		'detection_time'
	]
	for col in date_columns:
		#Преобразовываем в datetome и сохраняем в ту же строчку.
		df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

	#Числовые поля , к-е могут быть пустыми
	numeric_columns = ["total_kms", "speed_km_h"]
	for col in numeric_columns:
		df[col] = pd.to_numeric(df[col], errors="coerce")
	
	#Целочисленные столбцы:
	ids_columns = ["track_id", "tracker_id", "video_id"]
	for col in ids_columns:
		df[col] = df[col].fillna(0).astype(int)

	columns = [
		'video_id',
		'title',
		'path',
		'recording_at',
		'recording_end',
		'track_id',
		'tracker_id',
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

	client.insert_df("raw_layer.csv_traffic_raw", df[columns])
	print(f"Загружено {len(df)} строк в raw_layer.csv_traffic_raw")


path = r'C:\IT\NEVA projects\transport_analyst_p3\full_tracking_data.csv'
load_csv_to_raw(path)
