from pyflink.datastream.functions import AggregateFunction
from models.taxi_loc_stats import TaxiLocStats

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

    def merge(self, other):
        self.departures += other.departures
        self.arrivals += other.arrivals
        self.total_passengers += other.total_passengers
        self.total_amount += other.total_amount
        return self

    def to_stats(self):
        return TaxiLocStats(self.departures, self.arrivals, self.total_passengers, self.total_amount)

class TaxiLocAggregator(AggregateFunction):
    def create_accumulator(self):
        return TaxiLocAccumulator()

    def add(self, value, accumulator):
        if value.start_stop == 0:
            accumulator.add_departure()
        elif value.start_stop == 1:
            accumulator.add_arrival(value.passenger_count, value.amount)
        return accumulator

    def get_result(self, accumulator):
        return accumulator.to_stats()

    def merge(self, a, b):
        return a.merge(b)
