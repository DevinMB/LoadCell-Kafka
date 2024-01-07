import logging
import threading
from kafka import KafkaProducer
import datetime
import json

class ThreadedKafkaLoggingHandler(logging.Handler):
    def __init__(self, broker_address, topic, device_name):
        super().__init__()
        self.producer = KafkaProducer(
            bootstrap_servers=[broker_address],
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )
        self.topic = topic
        self.device_name = device_name

    def emit(self, record):
        threading.Thread(target=self._emit_in_thread, args=(record,)).start()

    def _emit_in_thread(self, record):
        try:
            key = f"{self.device_name}".encode('utf-8')
            message = self.format(record)
            self.producer.send(self.topic, key=key, value=message)
            self.producer.flush()
        except Exception:
            self.handleError(record)

    def format(self, record):
        log_entry = {
            "level": record.levelname,
            "timestamp": int(record.created),
            "message": record.getMessage()
        }
        return json.dumps(log_entry)

def get_threaded_kafka_logger(broker_address, topic, device_name):
    logger = logging.getLogger('ThreadedKafkaLogger')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        kafka_handler = ThreadedKafkaLoggingHandler(broker_address, topic, device_name)
        logger.addHandler(kafka_handler)
    return logger
