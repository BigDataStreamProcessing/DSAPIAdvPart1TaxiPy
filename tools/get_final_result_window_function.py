from pyflink.datastream.functions import ProcessWindowFunction
from datetime import datetime, timedelta
from typing import Iterable
from models.result_data import ResultData

class GetFinalResultWindowFunction(ProcessWindowFunction):

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable  # Iterable[TaxiLocStats]
    ) -> Iterable[ResultData]:
        # Zainicjuj agregaty
        departures = 0
        arrivals = 0
        total_passengers = 0
        total_amount = 0.0

        for stats in elements:
            departures       += stats.departures
            arrivals         += stats.arrivals
            total_passengers += stats.total_passengers
            total_amount     += stats.total_amount

        # użyj:
        epoch = datetime(1970, 1, 1)
        window_start = epoch + timedelta(milliseconds=context.window().start)
        window_end = epoch + timedelta(milliseconds=context.window().end)

        # Utwórz wynik
        result_data = ResultData(
            borough=key,
            from_time=window_start,
            to_time=window_end,
            departures=departures,
            arrivals=arrivals,
            total_passengers=total_passengers,
            total_amount=total_amount
        )

        # Zwróć przez yield
        yield result_data
