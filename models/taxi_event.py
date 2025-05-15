from datetime import datetime
from pyflink.common.typeinfo import Types

class TaxiEvent:
    def __init__(self):
        self.trip_id = 0
        self.start_stop = 0
        self.timestamp = None
        self.location_id = 0
        self.passenger_count = 0
        self.trip_distance = 0.0
        self.payment_type = 0
        self.amount = 0.0
        self.vendor_id = 0

    @staticmethod
    def from_row(row):
        event = TaxiEvent()
        event.trip_id = row[0]
        event.start_stop = row[1]
        ts = row[2]
        if ts and ts > 0:
            try:
                event.timestamp = datetime.fromtimestamp(ts / 1000)
            except OSError as e:
                print(f"Invalid timestamp: {ts} -> setting None")
                event.timestamp = None
        else:
            event.timestamp = None
        event.location_id = row[3]
        event.passenger_count = row[4]
        event.trip_distance = row[5]
        event.payment_type = row[6]
        event.amount = row[7]
        event.vendor_id = row[8]
        return event

    def __repr__(self):
        timestamp_str = self.timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ") if self.timestamp else "null"
        return (
            f"TaxiEvent(trip_id={self.trip_id}, start_stop={self.start_stop}, "
            f"timestamp={timestamp_str}, location_id={self.location_id}, "
            f"passenger_count={self.passenger_count}, trip_distance={self.trip_distance}, "
            f"payment_type={self.payment_type}, amount={self.amount}, vendor_id={self.vendor_id})"
        )

# Typ odpowiadający strukturze TaxiEvent
TAXI_EVENT_TYPE = Types.ROW([
    Types.LONG(),           # trip_id
    Types.INT(),            # start_stop
    Types.LONG(),           # timestamp
    Types.INT(),            # location_id
    Types.INT(),            # passenger_count
    Types.DOUBLE(),         # trip_distance
    Types.INT(),            # payment_type
    Types.DOUBLE(),         # amount
    Types.INT()             # vendor_id
])