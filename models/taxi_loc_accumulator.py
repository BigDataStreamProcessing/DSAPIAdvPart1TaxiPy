from models.taxi_loc_stats import TaxiLocStats  # Upewnij się, że ścieżka jest poprawna

class TaxiLocAccumulator:
    def __init__(self):
        self.departures = 0
        self.arrivals = 0
        self.total_passengers = 0
        self.total_amount = 0.0

    def add_departure(self):
        self.departures += 1

    def add_arrival(self, passengers: int, amount: float):
        self.arrivals += 1
        self.total_passengers += passengers
        self.total_amount += amount

    def to_stats(self):
        return TaxiLocStats(
            departures=self.departures,
            arrivals=self.arrivals,
            total_passengers=self.total_passengers,
            total_amount=self.total_amount
        )

    def merge(self, other: 'TaxiLocAccumulator'):
        self.departures += other.departures
        self.arrivals += other.arrivals
        self.total_passengers += other.total_passengers
        self.total_amount += other.total_amount
        return self
