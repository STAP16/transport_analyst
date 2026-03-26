from ultralytics import YOLO
from confluent_kafka import Producer
import boto3
import cv2
import json
from datetime import datetime

TARGET_CLASSES = {0: "pedestrian", 1:"bicycle", 2: "car", 3:"motorcycle", 5: "bus", 7: "truck"}
VIDEO_PATH = "fallback2.mp4"
#TRACKER_ID
CAMERA_ID = 1
DIRECTION = 123.302
WIDTH = 2.666700
LENGTH = 5.833300
FRAMES_PER_SEC = 30
TOPIC = "detections_raw"

model = YOLO("yolo26n.pt")
config = {
	'bootstrap.servers': 'localhost:9092',
	'group.id': 'mygroup',
	'auto.offset.reset': 'earliest'
}
producer = Producer(config)
results = model.track(source="fallback2.mp4" ,stream=True)

def create_message(obj, path_in_minio):
	
	vehicle_type = TARGET_CLASSES[int(obj.cls)]
	track_id = int(obj.id)
	x1, y1, x2, y2 = obj.xyxy[0].tolist()
	x_cord_m = (x1 + x2) / 2
	y_cord_m = (y1 + y2) / 2
	width = WIDTH
	length = LENGTH
	path = path_in_minio
	direction = DIRECTION
	detection_time = datetime.now().isoformat()
	confidence = round(float(obj.conf), 2)
	
	message = {
		'camera_id': CAMERA_ID,
		'video_id': CAMERA_ID,
		'title': VIDEO_PATH,
		'track_id': track_id,
		'object_type': vehicle_type,
		'width': width,
		'length': length,
		'detection_time': detection_time,
		'x_cord_m': x_cord_m,
		'y_cord_m': y_cord_m,
		'direction': direction,
		'confidence': confidence,
	}

	return json.dumps(message)

def send_frame_to_s3(result, current_frame):
	BUCKET_NAME = "detection-data"

	s3 = boto3.client(
		"s3",
		endpoint_url="http://localhost:9000",
		aws_access_key_id="minioadmin",
		aws_secret_access_key="minioadmin"
	)
	# Загрузка из памяти

	frame_img = result.plot()
	object_name = f"camera_{CAMERA_ID}/{datetime.now().strftime('%Y-%m-%d')}/frame_{current_frame}.jpg"

	_, buffer = cv2.imencode('.jpg', frame_img)
	s3.put_object(
			Bucket=BUCKET_NAME,
			Key=object_name,
			Body=buffer.tobytes()
	)
	return object_name

def send_message(topic, message):
	producer.produce(topic, value=message)
	producer.flush()

frames = 0
try:
	for result in results:
		frames += 1
		is_send = frames % FRAMES_PER_SEC == 0
		for obj in result.boxes:
			if int(obj.id) is None:
				continue
			cls_id = int(obj.cls)
			if cls_id not in TARGET_CLASSES:
				continue

			if is_send:
				object_path = send_frame_to_s3(result, frames)
				message = create_message(obj, object_path)
				send_message(TOPIC, message)

except KeyboardInterrupt:
	print("Detection has been stopped")



		
