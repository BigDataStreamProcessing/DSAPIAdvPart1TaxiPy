from datetime import datetime

class ResultData:
    def __init__(self, borough: str, from_time: datetime, to_time: datetime,
                 departures: int, arrivals: int, total_passengers: int, total_amount: float):
        self.borough = borough
        self.from_time = from_time
        self.to_time = to_time
        self.departures = departures
        self.arrivals = arrivals
        self.total_passengers = total_passengers
        self.total_amount = total_amount

    def __str__(self):
        return (
            f"ResultData(borough='{self.borough}', from={self.from_time}, to={self.to_time}, "
            f"departures={self.departures}, arrivals={self.arrivals}, "
            f"total_passengers={self.total_passengers}, total_amount={self.total_amount})"
        )
