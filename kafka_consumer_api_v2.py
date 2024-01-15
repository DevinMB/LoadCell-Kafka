from flask import Flask, jsonify
from threading import Thread, Lock
import os
from kafka import KafkaConsumer
from message_handler import create_message_object
from datetime import timedelta, datetime
import pytz
from dotenv import load_dotenv
import json
from sit_handler import Sit
import logging
from sensor_data import SensorData
import time

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Flask app setup
app = Flask(__name__)

# Global variables to store sit times
global_last_sit = {"timestamp": 0, "duration": 0}
global_total_sit_time = 0
global_max_sit_time = {"timestamp": 0, "duration": 0}
global_sit_counter = {
    "total_sits": 0,
    "morning_sits": 0,
    "afternoon_sits": 0,
    "evening_sits": 0,
    "night_sits": 0
}
global_current_status: False
global_last_sensor_read = {"currently_sitting": False}

data_lock = Lock()

def format_timestamp(timestamp):
    # Assuming timestamp is in UTC, adjust to Detroit timezone
    utc_time = datetime.utcfromtimestamp(timestamp)
    eastern = pytz.timezone('America/Detroit')
    return utc_time.replace(tzinfo=pytz.utc).astimezone(eastern).strftime('%Y-%m-%d %I:%M:%S %p %Z')

def increment_sit_counter(start_timestamp):
    detroit_time = datetime.utcfromtimestamp(start_timestamp).replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Detroit'))
    hour = detroit_time.hour

    with data_lock:
        global_sit_counter["total_sits"] += 1
        if 6 <= hour < 12:
            global_sit_counter["morning_sits"] += 1
        elif 12 <= hour < 18:
            global_sit_counter["afternoon_sits"] += 1
        elif 18 <= hour < 24:
            global_sit_counter["evening_sits"] += 1
        elif 0 <= hour < 6:
            global_sit_counter["night_sits"] += 1

def seconds_to_dhms(seconds):
    td = timedelta(seconds=seconds)
    return {
        "days": td.days,
        "hours": td.seconds // 3600,
        "minutes": (td.seconds // 60) % 60,
        "seconds": td.seconds % 60
    }

def kafka_live_sit_consumer_thread():
    global global_last_sensor_read
    
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
            print(sensor_read.to_json())
            if sensor_read.sit_status:
                print(f"sit detected")
                read_formated = {
                            "currently_sitting": sensor_read.sit_status,
                            "sat_down_at": format_timestamp(sensor_read.timestamp),
                            "current_sit_duration": seconds_to_dhms(int(time.time()) - sensor_read.timestamp)
                        }
                with data_lock:
                    global_last_sensor_read = read_formated
            else:
                with data_lock:
                    global_last_sensor_read = {"currently_sitting": False}
            
    except Exception as e:
        logging.error(f"Error in Kafka consumer thread: {e}")

def kafka_consumer_thread():
    global global_last_sit
    global global_total_sit_time 
    global global_max_sit_time
    global global_sit_counter
    global global_current_status
    total_sit_time = 0

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
            # print(sit.to_json())
            
            total_sit_time += sit.sit_duration
            
            with data_lock:
                global_last_sit = {
                        "timestamp": sit.start_timestamp,
                        "duration": sit.sit_duration
                    }
                global_total_sit_time = total_sit_time

            if sit.sit_duration > global_max_sit_time["duration"]:
                with data_lock:
                    global_max_sit_time = {
                        "timestamp": sit.start_timestamp,
                        "duration": sit.sit_duration
                    }
                    
            increment_sit_counter(sit.start_timestamp)
            
    except Exception as e:
        logging.error(f"Error in Kafka consumer thread: {e}")

# Start Kafka consumer in a separate thread
Thread(target=kafka_consumer_thread, daemon=True).start()

# Thread(target=kafka_live_sit_consumer_thread, daemon=True).start()


@app.route('/sit-stats/chair-sensor-1', methods=['GET'])
def get_sit_times():
    with data_lock:
        return jsonify({
            "currentSitStatus": global_last_sensor_read,
            "lastSitTime": {
                "timestamp": format_timestamp(global_last_sit["timestamp"]),
                "duration": seconds_to_dhms(global_last_sit["duration"])
            },
            "totalSitTime": seconds_to_dhms(global_total_sit_time),
            "maxSitTime": {
                "timestamp": format_timestamp(global_max_sit_time["timestamp"]),
                "duration": seconds_to_dhms(global_max_sit_time["duration"])
            },
            "sitCounter": global_sit_counter
        })

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
