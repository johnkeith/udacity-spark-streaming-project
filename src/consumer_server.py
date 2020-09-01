from kafka import KafkaConsumer
import json
import time

from kafka_server import TOPIC_NAME, BOOTSTRAP_SERVER

if __name__ == "__main__":
    consumer = KafkaConsumer(
      TOPIC_NAME,
      bootstrap_servers  = BOOTSTRAP_SERVER,
      auto_offset_reset  = 'earliest',
      enable_auto_commit = True,
      group_id           = 'policing-stats-group',
      value_deserializer = lambda v: json.loads(v.decode('utf-8'))
    )

    while True:
      for message in consumer:
        print(message.value)
