import producer_server
import json

TOPIC_NAME        = "departments.sf.calls"
BOOTSTRAP_SERVER = "localhost:9092"

def run_kafka_server():
    input_file = "./data/police-department-calls-for-service.json"

    producer = producer_server.ProducerServer(
        input_file        = input_file,
        topic             = TOPIC_NAME,
        bootstrap_servers = BOOTSTRAP_SERVER,
        client_id         = "policing-stats",
        value_serializer  = lambda v: json.dumps(v).encode('utf-8')
    )

    return producer

def feed():
    producer = run_kafka_server()
    producer.generate_data()

if __name__ == "__main__":
    feed()
