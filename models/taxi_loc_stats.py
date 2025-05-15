class TaxiLocStats:
    def __init__(self, departures: int, arrivals: int, total_passengers: int, total_amount: float):
        self.departures = departures
        self.arrivals = arrivals
        self.total_passengers = total_passengers
        self.total_amount = total_amount

    def __str__(self):
        return (
            f"TaxiLocStats(departures={self.departures}, arrivals={self.arrivals}, "
            f"total_passengers={self.total_passengers}, total_amount={self.total_amount})"
        )
