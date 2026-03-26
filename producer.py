import json
import boto3
import cv2
from datetime import datetime
from ultralytics import YOLO
from confluent_kafka import Producer

CAMERA_ID = "cam_1"
TOPIC = "detections_raw"
TARGET_CLASSES = {0: "person", 1: "bicycle", 2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}
VIDEO_FPS = 30

config = {
	'bootstrap.servers': '127.0.0.1:9092',
}

video_path = r"C:\IT\NEVA projects\transport_analyst_p2\vk_camera_2h_20260322_165003_part_006.mp4"
model = YOLO("yolo11n.pt")
producer = Producer(config)


def serialize_data(data):
	return json.dumps(data)

def send_message(topic, data):
	producer.produce(topic, value=data)

def send_frame_to_s3(result, current_frame):
	BUCKET_NAME = "raw-video"

	s3 = boto3.client(
		"s3",
		endpoint_url="http://localhost:9000",
		aws_access_key_id="minioadmin",
		aws_secret_access_key="minioadmin"
	)

	# Загрузка из памяти

	frame_img = result.plot()
	object_name = f"{CAMERA_ID}/{datetime.now().strftime('%Y-%m-%d')}/frame_{current_frame}.jpg"

	_, buffer = cv2.imencode('.jpg', frame_img)
	s3.put_object(
			Bucket=BUCKET_NAME,
			Key=object_name,
			Body=buffer.tobytes()
	)

def create_data(obj, frame: int):

	track_id = int(obj.id[0])
	conf = round(float(obj.conf[0]), 2)
	class_id = int(obj.cls[0])
	vehicle_type = TARGET_CLASSES[class_id]
	x1, y1, x2, y2 = obj.xyxy[0].tolist()
	timestamp = datetime.now().isoformat()
	path_to_frame = f"{CAMERA_ID}/{frame}/{track_id}/{timestamp}"

	data = {
		"camera_id": CAMERA_ID,
		"vehicle_type": vehicle_type,
		"confidence": conf,
		"track_id": track_id,
		"bbox_x1": x1,
		"bbox_y1": y1,
		"bbox_x2": x2,
		"bbox_y2": y2,
		"detection_path": path_to_frame,
		"detected_at": timestamp
	}

	return serialize_data(data)

results = model.track(video_path, stream=True)
current_frame = 0
try:
	for result in results:
		current_frame += 1
		for obj in result.boxes:

			if obj.id is None:
				continue
			
			class_id = int(obj.cls[0])
			if class_id not in TARGET_CLASSES:
				continue

			data = create_data(obj, current_frame)
			send_message(TOPIC, data)
		producer.flush()
		if current_frame % VIDEO_FPS == 0:
			send_frame_to_s3(result, current_frame)

except KeyboardInterrupt:
	print("stopped detections")