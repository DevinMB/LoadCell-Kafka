import json
import time
import logging
from flask import jsonify
from utility import Utility

class SensorData:
    def __init__(self, device_id, sit_status, avg_value=None):
        self.timestamp = int(time.time())
        self.device_id = device_id
        self.sit_status = sit_status
        self.avg_value = avg_value

    def to_json(self):
        data = {
            "timestamp": self.timestamp,
            "deviceId": self.device_id,
            "sitStatus": self.sit_status
        }
        if self.avg_value is not None:
            data["avgValue"] = self.avg_value

        return data
    
    def to_status(self):
        if self.sit_status:
            return {
                "currently_sitting": self.sit_status,
                "sat_down_at": Utility.format_timestamp(self.timestamp),
                "current_sit_duration": Utility.seconds_to_dhms(self.timestamp - int(time.time()))
            }
        else:
            return {
                "currently_sitting": self.sit_status
            }

    @classmethod
    def from_json(cls, json_input):
        try:
            # Check if json_input is a string and convert to dictionary if it is
            if isinstance(json_input, str):
                data = json.loads(json_input)
            elif isinstance(json_input, dict):
                data = json_input
            else:
                raise TypeError("Input must be a JSON string or a dictionary")

            # Check for required keys
            required_keys = ['deviceId', 'sitStatus']
            if not all(key in data for key in required_keys):
                raise ValueError("JSON object does not have all required keys for a 'sit object'")

            # Extracting data from JSON
            timestamp = data.get('timestamp', int(time.time()))
            device_id = data['deviceId']
            sit_status = data['sitStatus']
            avg_value = data.get('avgValue')

            # Create a new SensorData instance
            sensor_data = cls(device_id, sit_status, avg_value)
            sensor_data.timestamp = timestamp  
            return sensor_data

        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
            logging.error(f"Error processing JSON: {e}")
            return None
    

#     # Usage
# sensor_data = SensorData("chair-sensor-1", False)
# print(sensor_data.to_json())
    
#     # Usage example
# json_str = '{"timestamp": 1640995200, "deviceId": "chair-sensor-1", "sitStatus": true, "avgValue": 75.5}'
# sensor_data = SensorData.from_json(json_str)
# print(sensor_data.to_json())