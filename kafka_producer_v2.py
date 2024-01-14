import datetime
import os
import logging
from dotenv import load_dotenv
from kafka import KafkaProducer
import json

load_dotenv()

class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )

    def delivery_report(self, err, msg):
        if err is not None:
            logging.error('Message delivery failed: {}'.format(err))
        else:
            logging.info('Message delivered to {} [{}]'.format(msg.topic, msg.partition))

    def produce_message(self, topic, key, value):
        key = f"{key}".encode('utf-8')
        future = self.producer.send(topic, key=key, value=value)
        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
            self.delivery_report(None, record_metadata)
        except Exception as e:
            self.delivery_report(e, None)
        finally:
            self.producer.flush()
