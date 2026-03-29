def explore_data():
	import pandas as pd
	csv_path = r"historic_data\traffic_full_30min_historical.csv"
	df = pd.read_csv(csv_path, parse_dates=["timestamp"])

	#Календарные признаки
	df["hour"] = df["timestamp"].dt.hour
	df["month"] = df["timestamp"].dt.month
	df["day"] = df["timestamp"].dt.day_of_week
	df["is_weekend"] = (df["day"] >= 5).astype(int)

	#Лаговые призанки
	df["speed_lag_1"] = df["avg_speed"].shift(1)
	df["speed_lag_2"] = df["avg_speed"].shift(2)
	df["speed_lag_6"] = df["avg_speed"].shift(6)
	df["speed_lag_48"] = df["avg_speed"].shift(48)

	df["intensity_lag_1"] = df["intensity_30min"].shift(6)
	df["intensity_lag_2"] = df["intensity_30min"].shift(6)
	df["intensity_lag_48"] = df["intensity_30min"].shift(48)

	#Сокльзящие средние
	df["speed_rolling_6"] = df["avg_speed"].shift(1).rolling(6, min_periods=1).mean()
	df["speed_rolling_24"] = df["avg_speed"].shift(1).rolling(24, min_periods=1).mean()

	df["intensity_rolling_6"] = df["avg_speed"].shift(1).rolling(6, min_periods=1).mean()

	#Целевые переменные
	df["target_30min"] = df["avg_speed"].shift(-1)
	df["target_60min"] = df["avg_speed"].shift(-2)
	df["target_120min"] = df["avg_speed"].shift(-4)

	#Собираем все колонки
	feature_cols = [
		#Календарные признаки
		'hour', 'month', 'day', 'is_weekend',
		#Лаги скорости	
		'speed_lag_1', 'speed_lag_2', 'speed_lag_6', 'speed_lag_48',
		#Лаги интенсивности
		"intensity_lag_1", "intensity_lag_2", "intensity_lag_48",
		#Сокльзящие средние
		"speed_rolling_6", "speed_rolling_24", "intensity_rolling_6",
		#Структура потока
		'cars', 'trucks', 'busses',
	]
	df_model = df.dropna(subset=feature_cols + ["target_30min", "target_60min", "target_120min"])
	return df_model, feature_cols