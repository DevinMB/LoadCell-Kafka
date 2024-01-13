
import time
import sys
import os
import logging 
from sensor_data import SensorData
from dotenv import load_dotenv
from kafka_producer import KafkaProducerWrapper

load_dotenv()

EMULATE_HX711=False
if not EMULATE_HX711:
    import RPi.GPIO as GPIO
    from hx711 import HX711
else:
    from emulated_hx711 import HX711

referenceUnit = -441

# Instantiate logging
logging.basicConfig(filename='example.log', level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Instantiate the Kafka Producer
BROKER = os.getenv('BROKER')
device_name = "chair-sensor-1"
topic = 'raw-sit-topic'
producer = KafkaProducerWrapper(BROKER, device_name)

def cleanAndExit():
    logging.info("Cleaning up and exiting.")
    if not EMULATE_HX711:
        GPIO.cleanup()
    sys.exit()

hx = HX711(6, 5)
hx.set_reading_format("MSB", "MSB")
hx.set_reference_unit(referenceUnit)
hx.reset()
hx.tare()

logging.info("Tare done! Add weight now...")

is_weight_above_600 = False
avg_value = None
total_weight = 0
readings_count = 0

while True:
    try:
        val = hx.get_weight(5)
        # logging.info(f"Weight value read: {val}")

        # Check if the weight crosses the threshold and update the flag
        if val < -600 and not is_weight_above_600:
            is_weight_above_600 = True
            sensorRead = SensorData(device_name, is_weight_above_600)
            producer.produce_message(topic, sensorRead.to_json)
            
        elif val >= -600 and is_weight_above_600:
            is_weight_above_600 = False
            sensorRead = SensorData(device_name, is_weight_above_600, avg_value)
            producer.produce_message(topic, sensorRead.to_json)
            avg_value=None
            total_weight=0
            readings_count=0

        if is_weight_above_600:
            total_weight += val
            readings_count += 1
            avg_value = total_weight / readings_count if readings_count else None

        time.sleep(1)

    except (KeyboardInterrupt, SystemExit):
        cleanAndExit()
    except Exception as e:
        logging.error("An error occurred: " + str(e))
        hx.power_down()
        time.sleep(1) # Delay for sensor stabilization
        hx.power_up()
        time.sleep(1) 
        # cleanAndExit()
