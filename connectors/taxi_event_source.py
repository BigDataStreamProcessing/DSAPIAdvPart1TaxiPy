from pyflink.datastream.functions import SourceFunction
from pyflink.java_gateway import get_gateway

class TaxiEventSourceFunction(SourceFunction):
    def __init__(self, directory_path: str, element_delay_millis: int, max_elements: int):
        # Pobranie klasy Java (z przykładowej paczki com.example)
        java_taxi_src = get_gateway().jvm.TaxiEventSource
        # Utworzenie instancji obiektu Java z argumentami
        j_source = java_taxi_src(directory_path, element_delay_millis, max_elements)
        # Inicjalizacja rodzica z obiektem Java
        super(TaxiEventSourceFunction, self).__init__(j_source)
