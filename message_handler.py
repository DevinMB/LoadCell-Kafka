# message_handler.py

import json

class Message:
    def __init__(self, level, timestamp, text):
        self.level = level
        self.timestamp = timestamp
        self.text = text

    def __str__(self):
        return f"Level: {self.level}, Timestamp: {self.timestamp}, Text: {self.text}"

    def is_light_on(self):
        return 'on' in self.text

    def is_light_off(self):
        return 'off' in self.text

def create_message_object(json_message):
    # print(f"Raw message: {json_message}")

    # Check if the message starts with '{', indicating it's likely a JSON object
    if not json_message.strip().startswith('{'):
        # print("Message not in JSON format. Skipping.")
        return None

    try:
        message_data = json.loads(json_message)
        return Message(
            message_data.get('level'),
            message_data.get('timestamp'),
            message_data.get('message')
        )
    except json.JSONDecodeError:
        print("Error decoding JSON. Skipping.")
        return None

