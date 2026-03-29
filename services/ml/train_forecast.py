def train_forecast():
	import os
	import lightgbm as lgb
	import joblib
	import numpy as np
	from sklearn.metrics import mean_absolute_error 
	from services.ml.explore_data import explore_data

	df_model, feature_cols = explore_data()

	#СПЛИТ ПО ВРЕМЕНИ
	split_idx = int((len(df_model) * 0.8))
	train = df_model.iloc[:split_idx]
	test = df_model.iloc[split_idx:]

	X_train = train[feature_cols]
	X_test = test[feature_cols]

	models = {}
	models_data = [
		("30min", "target_30min"),
		("60min", "target_60min"),
		("120min", "target_120min")		
	]

	for horizon, target in models_data:
		y_train = train[target]
		y_test = test[target]

		#Настраиваем обучение
		model = lgb.LGBMRegressor(
			n_estimators = 500,
			learning_rate=0.05,
			num_leaves=31,
			verbose=-1
		)

		#Контролируем качество
		model.fit(
			X_train, y_train,
			eval_set=[(X_test, y_test)],
			callbacks=[lgb.early_stopping(50), lgb.log_evaluation(100)]
		)

		y_pred = model.predict(X_test)
		mae = mean_absolute_error(y_test, y_pred)
		rmse = np.sqrt(((y_test - y_pred) ** 2).mean())
		print(f"model: {horizon}, MAE: {mae}, RMSE: {rmse}")
		#Записываем модель
		models[horizon] = model
	
	os.makedirs("models", exist_ok=True)
	for name, model in models.items():
		path = f'models/lgb_{name}.joblib'
		joblib.dump(model, path)
		print(f"Сохранено: {path}")

train_forecast()

	