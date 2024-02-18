import unittest
from unittest.mock import patch, MagicMock
from kafka_sit_producer_v2 import process_message
from sensor_data import SensorData
import json

class TestKafkaProcessor(unittest.TestCase):

    @patch('kafka.KafkaConsumer')
    @patch('kafka.KafkaProducer')
    def test_process_message(self, mock_kafka_producer, mock_kafka_consumer):
        # Create mock messages to be consumed
        mock_message_1 = MagicMock()
        sensor_data_1 = SensorData("chair-sensor-test", True)
        sensor_data_1.timestamp = 1705101503
        mock_message_1.value = sensor_data_1.to_json().encode('utf-8')
        mock_message_1.key = "chair-sensor-test-device".encode('utf-8')
        mock_message_1.topic = 'raw-sit-topic'
        mock_message_1.partition = 0
        mock_message_1.offset = 123

        mock_message_2 = MagicMock()
        sensor_data_2 = SensorData("chair-sensor-test", False, 75.15)
        sensor_data_2.timestamp = 1705101505
        mock_message_2.value = sensor_data_2.to_json().encode('utf-8')
        mock_message_2.key = "chair-sensor-test-device".encode('utf-8')
        mock_message_2.topic = 'raw-sit-topic'
        mock_message_2.partition = 0
        mock_message_2.offset = 124

        consumer_topic_name = 'raw-sit-topic'
        producer_topic_name = 'sit-topic'

        # Mock the Kafka consumer to return these messages
        mock_kafka_consumer.return_value.__iter__.return_value = iter([mock_message_1, mock_message_2])

        # Mock the Kafka producer to capture messages sent to it
        mock_producer_send = mock_kafka_producer.return_value.send
        mock_producer_send.return_value = MagicMock()  # Mock the future returned by producer.send

        # Variables to track state within process_message
        last_state_status = False
        last_state_time = 0

        # Run the message processing function for each mock message
        for message in [mock_message_1, mock_message_2]:
            last_state_status, last_state_time = process_message(message=message, last_status=last_state_status, last_time=last_state_time, producer=mock_kafka_producer, consumer=mock_kafka_consumer)

        # Check if the producer produced the correct message
        message_1_data = json.loads(mock_message_1.value.decode('utf-8'))
        message_2_data = json.loads(mock_message_2.value.decode('utf-8'))

        expected_produced_message = {
            "start_timestamp": message_1_data['timestamp'],
            "end_timestamp": message_2_data['timestamp'],
            "device_id": "chair-sensor-test-device",
            "sit_duration": message_2_data['timestamp'] - message_1_data['timestamp'],
            "time_bucket": 'afternoon',
            "avg_value": 75.15
        }

        mock_producer_send.assert_called_with(
            'sit-topic', 
            key=b'chair-sensor-test-device', 
            value=json.dumps(expected_produced_message).encode('utf-8')
        )

if __name__ == '__main__':
    unittest.main()

