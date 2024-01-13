import json
import time
import logging


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
            "sitStatus": self.sit_status,
            "avgValue": self.avg_value
        }
        return json.dumps({k: v for k, v in data.items() if v is not None})
    
    @classmethod
    def from_json(cls, json_str):
        try:
            data = json.loads(json_str)

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

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logging.error(f"Error processing JSON: {e}")
            return None
    

#     # Usage
# sensor_data = SensorData("chair-sensor-1", False)
# print(sensor_data.to_json())
    
#     # Usage example
# json_str = '{"timestamp": 1640995200, "deviceId": "chair-sensor-1", "sitStatus": true, "avgValue": 75.5}'
# sensor_data = SensorData.from_json(json_str)
# print(sensor_data.to_json())