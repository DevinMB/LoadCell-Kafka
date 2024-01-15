from flask import Flask, jsonify
from threading import Thread
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv
from sit_handler import Sit
import logging
from sensor_data import SensorData
from sit_data_manager import SitDataManager

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Flask app setup
app = Flask(__name__)

# Initialize Data Manager
sit_data_manager = SitDataManager()

def live_sits_consumer():
    
    # Kafka Consumer Configuration
    bootstrap_servers = [os.getenv('BROKER')]  # Replace with your Kafka server addresses
    topic_name = 'raw-sit-topic'  # Replace with your topic name

    # Create a Kafka Consumer
    consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start from the earliest messages
    group_id='sit-counter-api-4',  # Consumer group ID
    enable_auto_commit=True
    )

    try:
        for message in consumer:
            key = message.key.decode('utf-8')
            sensor_read = SensorData.from_json(message.value.decode('utf-8'))
            print(f"{sensor_read.to_json()}")
            sit_data_manager.update_current_sit_status(sensor_read.to_status()) 
    except Exception as e:
        logging.error(f"Error in Kafka consumer thread: {e}")

def sit_agregation_consumer():

    # Kafka Consumer Configuration
    bootstrap_servers = [os.getenv('BROKER')]  # Replace with your Kafka server addresses
    topic_name = 'sit-topic'  # Replace with your topic name

    # Create a Kafka Consumer
    consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start from the earliest messages
    group_id='sit-counter-api-4',  # Consumer group ID
    enable_auto_commit=False
    )

    try:
        for message in consumer:
            key = message.key.decode('utf-8')
            sit = Sit.from_json(message.value.decode('utf-8'))
            # print(f"{sit.to_json()}")
            sit_data_manager.add_sit_event(sit)
    except Exception as e:
        logging.error(f"Error in Kafka consumer thread: {e}")

# Start Kafka consumers in separate threads
Thread(target=live_sits_consumer, daemon=True).start()
Thread(target=sit_agregation_consumer, daemon=True).start()


@app.route('/sit-stats/<device_id>', methods=['GET'])
def get_sit_times(device_id):
        
        return jsonify(sit_data_manager.get_device_sit_statistics(device_id))


if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
