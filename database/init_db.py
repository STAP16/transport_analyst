from database.client import get_client

from database.DWH import mart_layer as mart
from database.DWH import staging_layer as staging
from database.DWH import raw_layer as raw

client = get_client()

def init_db():
	DATABASES = ["raw_layer", "staging_layer", "mart_layer"]
	for db in DATABASES:
		client.query(f"DROP DATABASE IF EXISTS {db}")
		client.query(f"CREATE DATABASE IF NOT EXISTS {db}")
		print(f"DATABASE: {db} has been created")

	#-- raw_layer
	raw.create_raw_detections_table()
	raw.create_csv_traffic_raw()
	#-- staging_layer
	staging.create_detections_clean_table()
	#-- mart_layer
	mart.create_realtime_traffic()
	mart.create_hourly_traffic()
	mart.create_system_apperances()
	mart.create_proximity_events()
	mart.create_dispatcher_predictions()
	
	print("All tables has been created")


init_db()