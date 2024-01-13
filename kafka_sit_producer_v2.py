import os
from kafka import KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata
from message_handler import create_message_object
from dotenv import load_dotenv
import json
from sit_handler import Sit
from sensor_data import SensorData
load_dotenv()

bootstrap_servers = [os.getenv('BROKER')] 
consumer_topic_name = 'raw-sit-topic' 
producer_topic_name = 'sit-topic'  

consumer = KafkaConsumer(
    consumer_topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    group_id='sit-producer-1',
    enable_auto_commit=False
)

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

last_state_status = False
last_state_time = 0

print(f"Sit-Producer Running...")

try:
    for message in consumer:
        key = message.key.decode('utf-8')
        message_object = SensorData.from_json(message.value.decode('utf-8'))
        if message_object is not None:
            if message_object.sit_status: 
                print(f"Person detected sitting at {message_object.timestamp}")
                last_state_status = True
                last_state_time = message_object.timestamp
            if message_object.sit_status(): 
                print(f"Person detected getting up at  {message_object.timestamp}")
                if last_state_status:
                    difference_in_seconds = message_object.timestamp - last_state_time

                    #produce a sit object to the sit topic
                    sit = Sit(
                    start_epoch=last_state_time,
                    end_epoch=message_object.timestamp,
                    device_id=key,
                    sit_duration=difference_in_seconds,
                    avg_value=message_object.avg_value
                    )
                    producer.send(producer_topic_name, key=key.encode('utf-8'), value=sit.to_json())
                    
                    tp = TopicPartition(message.topic, message.partition)
                    offsets = {tp: OffsetAndMetadata(message.offset + 1, None)}
                    consumer.commit(offsets=offsets)
                    
                    last_state_status = False   

except Exception as e:
    print(f"Error in Kafka sit producer: {e}")


