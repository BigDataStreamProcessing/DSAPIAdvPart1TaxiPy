from datetime import datetime

class TaxiLocEvent:
    def __init__(self, borough: str, location_id: int, timestamp: datetime,
                 start_stop: int, passenger_count: int, amount: float):
        self.borough = borough
        self.location_id = location_id
        self.timestamp = timestamp
        self.start_stop = start_stop
        self.passenger_count = passenger_count
        self.amount = amount

    def __repr__(self):
        return (
            f"TaxiLocEvent(borough='{self.borough}', location_id={self.location_id}, "
            f"timestamp={self.timestamp}, start_stop={self.start_stop}, "
            f"passenger_count={self.passenger_count}, amount={self.amount})"
        )
