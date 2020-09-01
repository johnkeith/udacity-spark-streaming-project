from kafka import KafkaProducer
import json
import time

class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic      = topic

    def generate_data(self):
        with open(self.input_file) as file:
            json_contents = json.load(file)
            for record in json_contents:
                self.send(
                    topic = self.topic,
                    value = record
                )

                time.sleep(0.5)
