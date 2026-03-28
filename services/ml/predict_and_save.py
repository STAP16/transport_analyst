def predict_and_save():
	from services.ml.save_to_clickhouse import save_to_clickhouse
	from services.ml.explore_data import explore_data
	import joblib

	df_model, feature_cols = explore_data()
	recent = df_model.tail(100).copy()
	X = recent[feature_cols]
	

	models = {
		'30min': joblib.load('models/lgbm_speed_30min.joblib'),
		'60min': joblib.load('models/lgbm_speed_60min.joblib'),
		'120min': joblib.load('models/lgbm_speed_120min.joblib'),
	}

	# Прогнозы от трёх моделей
	recent['avg_speed_kmh30'] = models['30min'].predict(X)
	recent['avg_speed_kmh60'] = models['60min'].predict(X)
	recent['avg_speed_kmh120'] = models['120min'].predict(X)

	# Определяем пробку (порог 20 км/ч)
	recent['is_traffic_jam30'] = (recent['avg_speed_kmh30'] < 20).astype(int)
	recent['is_traffic_jam60'] = (recent['avg_speed_kmh60'] < 20).astype(int)
	recent['is_traffic_jam120'] = (recent['avg_speed_kmh120'] < 20).astype(int)


	recent = recent.drop_duplicates(subset=['timestamp'])
	save_to_clickhouse(recent)

predict_and_save()