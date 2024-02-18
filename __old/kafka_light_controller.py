import os
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaTimeoutError
from sit_handler import Sit
from sensor_data import SensorData
from hue_controller import HueController
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Hue Controller setup
BRIDGE_IP = os.getenv('BRIDGE_IP')
USER_TOKEN = os.getenv('USER_TOKEN')
light_id = 47
hue = HueController(BRIDGE_IP, USER_TOKEN)

# Kafka Consumer Configuration
bootstrap_servers = [os.getenv('BROKER')]
raw_sit_topic = 'raw-sit-topic'
sit_topic = 'sit-topic'

# Function to process sit counts
def process_sit_count(sit_counter):
    if sit_counter >= 100:
        sit_counter %= 100  # Reset counter, keeping remainder
    return sit_counter

# Aggregate Sits Consumer
total_sit_aggregator = KafkaConsumer(
    sit_topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    group_id='light-controller-1',
    enable_auto_commit=False
)

# Get end offset for Sits Aggregator
partition = 0  # Assuming there is only one partition and it's partition 0
tp = TopicPartition(sit_topic, partition)
end_offset = total_sit_aggregator.end_offsets([tp])[tp]


timeout_duration = 10  #seconds
sit_counter = 0

# Aggregate sit counts
try:
    for message in total_sit_aggregator:
        key = message.key.decode('utf-8')
        sit = Sit.from_json(message.value.decode('utf-8'))
        current_offset = message.offset
        
        if key == 'chair-sensor-1' and sit is not None:
            sit_counter += 1
            sit_counter = process_sit_count(sit_counter)
        
        if current_offset >= end_offset - 1:
            print("Reached end of the available messages, moving on.")
            break
finally:
    total_sit_aggregator.close()


print(f"Aggregated total of {sit_counter} sits.")

if sit_counter >= 100: 
    if sit_counter == 100:
        sit_counter = 0
        print(f"Sit counter reset to 0. Suprise upcoming at next 100 sits.")
    else: 
        sit_counter = sit_counter % 100
        print(f"Sit counter reset to {sit_counter}. Suprise at next 100 sits.")
else:
    print("Suprise upcoming at 100 sits.")

print(f"Now reading from live sits...")

# Live Sits Consumer
live_sit_consumer = KafkaConsumer(
    raw_sit_topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    group_id='light-controller-1',
    enable_auto_commit=True
)

# Process live sits
try:
    for message in live_sit_consumer:
        key = message.key.decode('utf-8')
        sensor_read = SensorData.from_json(message.value.decode('utf-8'))

        if key == 'chair-sensor-1' and sensor_read is not None and sensor_read.sit_status:
            sit_counter += 1
            hue.turn_on_light(light_id)
            print(f"Sit Down Event Received. Sit counter is: {sit_counter}")

            if sit_counter == 100:
                print("100 SITS ACHIEVED VERY POGGERS")
                sit_counter = 0

        elif sensor_read is not None and not sensor_read.sit_status:
            hue.turn_off_light(light_id)
            print("Sit Up Event Received")
except KeyboardInterrupt:
    print("Consumer stopped.")
except Exception as e:
    print(f"Error: {e}")
finally:
    live_sit_consumer.close()
