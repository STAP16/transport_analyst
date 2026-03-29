def save_to_clickhouse(recent):
	from database.client import get_client
	import numpy as np
	import pandas as pd
	
	client = get_client()
	#Собираем все данные из recent в dataframe
	
	result = pd.DataFrame({
		"camera_id": 1,
		"video_id": 1,
		'predicted_at': recent["timestamp"],
		'width': 0.0,
		'length': 0.0,
		'total_vehicles': recent["intensity_30min"].astype('uint32') ,
		'avg_speed_kmh30': recent['avg_speed_kmh30'].astype('float32'),
		'avg_speed_kmh60': recent['avg_speed_kmh60'].astype('float32'),
		'avg_speed_kmh120': recent['avg_speed_kmh120'].astype('float32'),
		'is_traffic_jam30': recent['is_traffic_jam30'].astype('uint8'),
		'is_traffic_jam60': recent['is_traffic_jam60'].astype('uint8'),
		'is_traffic_jam120': recent['is_traffic_jam120'].astype('uint8'),
		'is_overdrawing': np.uint8(0),
		'is_holiday': np.uint8(0),
		'is_peak_hour': recent["is_peak_hour"].astype('uint8') if "is_peak_hour" in recent.columns else np.uint8(0),
		'recomendation': "Без рекомендации",
		'expected_result': "Стабильно",
		'direction': "all",
	})

	client.insert_df("mart_layer.dispatcher_predictions", result)
	print("✔️ Successful inserted predictions to dispatcher_predictions ")
