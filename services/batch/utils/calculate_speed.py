def calculate_speed(first_track_point, last_track_point):
	import numpy as np
	dist = np.sqrt(
		(last_track_point["x_cord_m"] - first_track_point["x_cord_m"])**2 +
		(last_track_point["y_cord_m"] - first_track_point["y_cord_m"])**2
	)

	time_diff = (last_track_point["detection_time"] - first_track_point["detection_time"]).dt.total_seconds()

	speed_km_h = (dist/time_diff) * 3.6
	total_kms = dist / 1000
	speed_km_h = np.clip(speed_km_h, 0, 200)
	return speed_km_h, total_kms