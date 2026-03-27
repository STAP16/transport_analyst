from confluent_kafka import Consumer
from datetime import datetime
from database.client import get_client
import time
import json

POLL_TIMEOUT = 5
INSERT_CONST = 300
TIME_TO_INSERT_SEC = 5

config = {
	'bootstrap.servers': 'localhost:9092',
	'group.id': 'analyst_group',
	'auto.offset.reset': 'latest'
}
client = get_client()

def insert_message_to_raw_layer(batch: list[object]):
	column_names = [
		'camera_id',
		'video_id',
		'title',
		'track_id',
		'object_type',
		'width',
		'length',
		'detection_time',
		'x_cord_m',
		'y_cord_m',
		'direction',
		'confidence'
	]
	rows = []
	for item in batch:
		row = (
			item['camera_id'],
			item['video_id'],
			item['title'],
			item['track_id'],
			item['object_type'],
			item['width'],
			item['length'],
			item['detection_time'],
			item['x_cord_m'],
			item['y_cord_m'],
			item['direction'],
			item['confidence']
		)
		rows.append(row)
	client.insert("raw_layer.raw_detections",rows, column_names)
	batch.clear()

consumer = Consumer(config)
consumer.subscribe(["detections_raw"])
batch = []
last_insert_time = 0
try:
	while True:
		msg = consumer.poll(POLL_TIMEOUT)
		print(msg)
		if msg is None:
			if len(batch) > 0 and time.time() - last_insert_time <= INSERT_CONST:
				insert_message_to_raw_layer(batch)
				last_insert_time = time.time()
			continue

		print("len batch", len(batch))
		batch.append(json.loads(msg.value()))	
		if len(batch) >= INSERT_CONST:
			insert_message_to_raw_layer(batch)
			print("Вставка в таблицу")
			last_insert_time = time.time()

except KeyboardInterrupt:
	if len(batch) > 0:
		insert_message_to_raw_layer(batch)
	print("Consumer has been stopped")
