def train_model():
	import lightgbm as lgb
	from services.ml.explore_data import explore_data
	import numpy as np
	from sklearn.metrics import mean_absolute_error
	import joblib
	import os
	df_model, feature_cols = explore_data()

	# === СПЛИТ ПО ВРЕМЕНИ: 80% train, 20% test ===
	split_idx = int(len(df_model) * 0.8)
	train = df_model.iloc[:split_idx]
	test = df_model.iloc[split_idx:]

	print(f"Train: {len(train)} строк ({train['timestamp'].min()} — {train['timestamp'].max()})")
	print(f"Test:  {len(test)} строк ({test['timestamp'].min()} — {test['timestamp'].max()})")

	X_train = train[feature_cols]
	X_test = test[feature_cols]

    # === ВСЕ ТРИ ГОРИЗОНТА ===
	models = {}
	for horizon, target_name in [('30min', 'target_30min'), 
																('60min', 'target_60min'), 
																('120min', 'target_120min')]:
		y_train = train[target_name]
		y_test = test[target_name]

		model = lgb.LGBMRegressor(
				n_estimators=500,
				learning_rate=0.05,
				num_leaves=31,
				min_child_samples=20,
				verbose=-1,
		)

		model.fit(
				X_train, y_train,
				eval_set=[(X_test, y_test)],
				callbacks=[lgb.early_stopping(50), lgb.log_evaluation(100)]
		)

		y_pred = model.predict(X_test)
		mae = mean_absolute_error(y_test, y_pred)
		rmse = np.sqrt(((y_test - y_pred) ** 2).mean())

		print(f"\n{horizon}: MAE={mae:.2f}, RMSE={rmse:.2f}")
		models[horizon] = model


	os.makedirs('models', exist_ok=True)
	for name, model in models.items():
		path = f'models/lgbm_speed_{name}.joblib'
		joblib.dump(model, path)
		print(f"Сохранено: {path}")

	return models