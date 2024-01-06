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
            value_serializer=lambda m: json.dumps(m).encode('ascii')
        )
        self.topic = topic
        self.device_name = device_name

    def emit(self, record):
        # Create and start a new thread for sending the log message
        threading.Thread(target=self._emit_in_thread, args=(record,)).start()

    def _emit_in_thread(self, record):
        try:
            # Create a message key and value
            current_time = datetime.datetime.now()
            key = f"{self.device_name}".encode('utf-8')
            message = self.format(record)

            # Produce and flush the message
            self.producer.send(self.topic, key=key, value=message)
            self.producer.flush()
        except Exception:
            self.handleError(record)

def get_threaded_kafka_logger(broker_address, topic, device_name):
    logger = logging.getLogger('ThreadedKafkaLogger')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        kafka_handler = ThreadedKafkaLoggingHandler(broker_address, topic, device_name)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        kafka_handler.setFormatter(formatter)
        logger.addHandler(kafka_handler)
    return logger
