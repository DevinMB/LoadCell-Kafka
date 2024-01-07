from kafka import KafkaConsumer
import json
from message_handler import create_message_object
from datetime import timedelta
from dotenv import load_dotenv
import os

load_dotenv()

# Kafka Consumer Configuration
bootstrap_servers = [os.getenv('BROKER')]  # Replace with your Kafka server addresses
topic_name = 'log-topic'  # Replace with your topic name

# Create a Kafka Consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start from the earliest messages
    group_id='sit-counter',  # Consumer group ID
    enable_auto_commit=False
)

def seconds_to_dhms(seconds):
    td = timedelta(seconds=seconds)
    return {
        "days": td.days,
        "hours": td.seconds // 3600,
        "minutes": (td.seconds // 60) % 60,
        "seconds": td.seconds % 60
    }



sit_counter = 0
last_state_status = False
last_state_time = seconds_to_dhms(sit_counter)

# Consume messages
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
                    sit_counter = sit_counter + difference_in_seconds
                last_state_status = False
                last_sit_time = seconds_to_dhms(difference_in_seconds)
                total_sit_time = seconds_to_dhms(sit_counter)
                print(f"Last Sit Time: {last_sit_time}")
                print(f"Total Sit Time: {total_sit_time}")

                
        else:
            print("Error occurred while processing message. Moving to next message.")



except KeyboardInterrupt:
    print("Consumer stopped.")
except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()
