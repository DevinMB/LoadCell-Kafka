from flask import Flask, jsonify
from threading import Thread, Lock
import os
from kafka import KafkaConsumer
from message_handler import create_message_object
from datetime import timedelta
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Flask app setup
app = Flask(__name__)

# Global variables to store sit times
sit_counter = 0
last_sit_time = 0
data_lock = Lock()

def seconds_to_dhms(seconds):
    td = timedelta(seconds=seconds)
    return {
        "days": td.days,
        "hours": td.seconds // 3600,
        "minutes": (td.seconds // 60) % 60,
        "seconds": td.seconds % 60
    }

def kafka_consumer_thread():
    global global_last_sit_time
    global global_total_sit_time 
    last_state_status = False
    last_state_time = 0
    total_sit_time = 0

    # Kafka Consumer Configuration
    bootstrap_servers = [os.getenv('BROKER')]  # Replace with your Kafka server addresses
    topic_name = 'log-topic'  # Replace with your topic name

    # Create a Kafka Consumer
    consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start from the earliest messages
    group_id='sit-counter-api',  # Consumer group ID
    enable_auto_commit=False
    )

    try:
        for message in consumer:
            key = message.key.decode('utf-8')
            message_object = create_message_object(message.value.decode('utf-8'))
            if key == 'chair-sensor-1' and message_object is not None:
                if message_object.is_light_on(): 
                    print(f"Light turned on at {message_object.timestamp}")
                    last_state_status = True
                    last_state_time = message_object.timestamp

                if message_object.is_light_off(): 
                    print(f"Light turned off at {message_object.timestamp}")
                    if last_state_status:
                        difference_in_seconds = message_object.timestamp - last_state_time
                        total_sit_time = total_sit_time + difference_in_seconds
                        
                        # Update global variables in a thread-safe manner
                        with data_lock:
                            global_last_sit_time = difference_in_seconds
                            global_total_sit_time = total_sit_time
                        last_state_status = False   
                        # print(f"Last Sit Time: {difference_in_seconds}")
                        # print(f"Total Sit Time: {total_sit_time}")

    except Exception as e:
        print(f"Error in Kafka consumer thread: {e}")

# Start Kafka consumer in a separate thread
Thread(target=kafka_consumer_thread, daemon=True).start()

@app.route('/sit-times', methods=['GET'])
def get_sit_times():
    with data_lock:
        return jsonify({
            "lastSitTime": seconds_to_dhms(global_last_sit_time),
            "totalSitTime": seconds_to_dhms(global_total_sit_time)
        })

if __name__ == '__main__':
    app.run(debug=True)
