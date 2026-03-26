import clickhouse_connect

def get_client():
	client = clickhouse_connect.get_client(
		host="localhost", 
		username="click", 
		database="analytics", 
		password="click", 
		port="8123"
	)
	return client



get_client()