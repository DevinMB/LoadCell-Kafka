import threading
from flask import Flask, jsonify
from datetime import datetime
import pytz
import json
from utility import Utility


class SitDataManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.current_sit_status = {"currently_sitting": False}
        self.sit_events = []  
    
    def update_current_sit_status(self, status):
        with self.lock:
            self.current_sit_status = status
    
    def add_sit_event(self, event):
        with self.lock:
            self.sit_events.append(event)
    
    def increment_sit_counter(self, sit_counter, timestamp):
        detroit_time = datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Detroit'))
        hour = detroit_time.hour

        sit_counter["total_sits"] += 1
        if 6 <= hour < 12:
            sit_counter["morning_sits"] += 1
        elif 12 <= hour < 18:
            sit_counter["afternoon_sits"] += 1
        elif 18 <= hour < 24:
            sit_counter["evening_sits"] += 1    
        elif 0 <= hour < 6:
            sit_counter["night_sits"] += 1
        
        return sit_counter
    
    def get_device_sit_statistics(self, device_name):
        
        last_sit = {"duration": 0,"timestamp": 0}
        max_sit = {"duration": 0,"timestamp": 0}
        total_sit_time = 0
        sit_counter = {
            "afternoon_sits": 0,
            "evening_sits": 0,
            "morning_sits": 0,
            "night_sits": 0,
            "total_sits": 0
        }
        for sit in self.sit_events:
            if sit.device_id == device_name:
                last_sit = {
                    "duration": Utility.seconds_to_dhms(sit.sit_duration),
                    "timestamp": Utility.format_timestamp(sit.start_timestamp)
                }
                if sit.sit_duration > max_sit["duration"]: 
                    max_sit = {"duration": sit.sit_duration, "timestamp": Utility.format_timestamp(sit.start_timestamp)}
                
                sit_counter = self.increment_sit_counter(sit_counter, sit.start_timestamp)

                total_sit_time += sit.sit_duration
            
            # for sit in self.sit_events:
                # print(sit.to_json())
        return {
            "currentSitStatus": self.current_sit_status,
            "lastSit": last_sit,
            "maxSit": max_sit,
            "sitCounter": sit_counter,
            "totalSitTime": Utility.seconds_to_dhms(total_sit_time)
        }

                
                

        