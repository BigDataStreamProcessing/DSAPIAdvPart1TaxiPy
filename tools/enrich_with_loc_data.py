import csv
from pyflink.datastream.functions import MapFunction, RuntimeContext

from models.taxi_event import TaxiEvent
from models.taxi_loc_event import TaxiLocEvent
from models.loc_data import LocData

class EnrichWithLocData(MapFunction):
    def __init__(self, loc_file_path: str):
        self.loc_file_path = loc_file_path
        self.loc_data_map = {}

    def open(self, runtime_context: RuntimeContext):
        # Załadowanie mapy loc_data raz przy starcie taska
        self.loc_data_map = self._load_loc_data_map()

    def map(self, taxi_event: TaxiEvent):

        location_id = taxi_event.location_id
        loc_data = self.loc_data_map.get(location_id)
        borough = loc_data.borough if loc_data else "Unknown"

        return TaxiLocEvent(
            borough=borough,
            location_id=taxi_event.location_id,
            timestamp=taxi_event.timestamp,
            start_stop=taxi_event.start_stop,
            passenger_count=taxi_event.passenger_count,
            amount=taxi_event.amount
        )

    def _load_loc_data_map(self):
        loc_data_map = {}
        with open(self.loc_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # pomiń header
            for parts in reader:
                if len(parts) != 4:
                    continue
                location_id = int(parts[0])
                borough     = parts[1].strip('"')
                zone        = parts[2].strip('"')
                service_zone= parts[3].strip('"')
                loc_data_map[location_id] = LocData(
                    location_id=location_id,
                    borough=borough,
                    zone=zone,
                    service_zone=service_zone
                )
        return loc_data_map
