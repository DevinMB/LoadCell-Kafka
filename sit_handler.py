from datetime import datetime, timedelta
import pytz

class Sit:
    def __init__(self, start_epoch, end_epoch, device_id, sit_duration, avg_value):
        self.start_timestamp = start_epoch
        self.end_timestamp = end_epoch
        self.device_id = device_id
        self.sit_duration = sit_duration
        self.time_bucket = self.determine_time_bucket(self.start_timestamp)
        self.avg_value = avg_value

    def determine_time_bucket(self, start_timestamp):
        detroit_time = datetime.utcfromtimestamp(start_timestamp).replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Detroit'))
        hour = detroit_time.hour

        if 6 <= hour < 12:
            return "morning"
        elif 12 <= hour < 18:
            return "afternoon"
        elif 18 <= hour < 24:
            return "evening"
        else:
            return "night"

    def seconds_to_dhms(seconds):
        td = timedelta(seconds=seconds)
        return {
        "days": td.days,
        "hours": td.seconds // 3600,
        "minutes": (td.seconds // 60) % 60,
        "seconds": td.seconds % 60
        }


    def to_json(self):
        return {
            "start_timestamp": self.start_timestamp,
            "end_timestamp" : self.end_timestamp,
            "device_id": self.device_id,
            "sit_duration": self.sit_duration,
            "time_bucket": self.time_bucket,
            "avg_value": self.avg_value
        }
