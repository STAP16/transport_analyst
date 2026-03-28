def explore_data():
	'''Подготовка данных к обучению деревьев бустинга'''

	from database.client import get_client

	import pandas as pd

	df = pd.read_csv(r'historic_data\traffic_full_30min_historical.csv', parse_dates=['timestamp'])
	#Календарные признаки
	df["hour"] = df["timestamp"].dt.hour
	df["day_of_week"] = df["timestamp"].dt.day_of_week
	df["month"] = df["timestamp"].dt.month
	df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

	#Лаговые признаки
	df["speed_lag_1"] = df["avg_speed"].shift(1)
	df["speed_lag_2"] = df["avg_speed"].shift(2)
	df["speed_lag_6"] = df["avg_speed"].shift(6)
	df["speed_lag_48"] = df["avg_speed"].shift(48)

	df['intensity_lag_1'] = df['intensity_30min'].shift(1)
	df['intensity_lag_2'] = df['intensity_30min'].shift(2)
	df['intensity_lag_48'] = df['intensity_30min'].shift(48)

	#Скользящие средние
	df['speed_rolling_6'] = df['avg_speed'].shift(1).rolling(6, min_periods=1).mean()
	df['speed_rolling_24'] = df['avg_speed'].shift(1).rolling(24, min_periods=1).mean()

	df['intensity_rolling_6'] = df['intensity_30min'].shift(1).rolling(6, min_periods=1).mean()
	

	# === ЦЕЛЕВЫЕ ПЕРЕМЕННЫЕ ===
	df['target_30min'] = df['avg_speed'].shift(-1)   # через 30 мин
	df['target_60min'] = df['avg_speed'].shift(-2)   # через 60 мин
	df['target_120min'] = df['avg_speed'].shift(-4)  # через 120 мин

	print(df[['timestamp', 'avg_speed', 'target_30min', 'target_60min', 'target_120min']].head(10))


	# === СОБИРАЕМ ВСЕ ПРИЗНАКИ В СПИСОК ===
	feature_cols = [
		# календарные
		'hour', 'day_of_week', 'month', 'is_weekend',
		# погода (уже есть в CSV)
		'temperature', 'precipitation',
		# лаги скорости
		'speed_lag_1', 'speed_lag_2', 'speed_lag_6', 'speed_lag_48',
		# лаги интенсивности
		'intensity_lag_1', 'intensity_lag_2', 'intensity_lag_48',
		# скользящие
		'speed_rolling_6', 'speed_rolling_24', 'intensity_rolling_6',
		# структура потока
		'cars', 'trucks', 'busses',
	]

	df_model = df.dropna(subset=feature_cols + ['target_30min', 'target_60min', 'target_120min'])
	return df_model, feature_cols

