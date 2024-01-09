from datetime import timedelta

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
