from datetime import timedelta, datetime
import pytz


class Utility:
    @staticmethod
    def seconds_to_dhms(seconds):
        """ Convert seconds to a dictionary of days, hours, minutes, and seconds. """
        td = timedelta(seconds=seconds)
        return {
            "days": td.days,
            "hours": td.seconds // 3600,
            "minutes": (td.seconds // 60) % 60,
            "seconds": td.seconds % 60
        }
    
    @staticmethod
    def format_timestamp(timestamp):
        utc_time = datetime.utcfromtimestamp(timestamp)
        eastern = pytz.timezone('America/Detroit')
        return utc_time.replace(tzinfo=pytz.utc).astimezone(eastern).strftime('%Y-%m-%d %I:%M:%S %p %Z')
