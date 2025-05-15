from pyflink.datastream.functions import ProcessWindowFunction
from datetime import datetime, timezone, timedelta
from typing import Iterable, TypeVar

T = TypeVar('T')

class DetInfoProcessWindowFunction(ProcessWindowFunction[T, str, str, object]):

    def process(
            self,
            key: str,
            context: ProcessWindowFunction.Context,
            elements: Iterable[T]
    ) -> Iterable[str]:
        # Zbuduj ciąg wartości
        values = ", ".join(str(e) for e in elements)

        # Konwersja milisekund na daty
        epoch = datetime(1970, 1, 1)
        start = epoch + timedelta(milliseconds=context.window().start)
        end = epoch + timedelta(milliseconds=context.window().end)

        # Zwróć pojedynczy wynik
        yield f"Window: {start}-{end}; Key: {key} values: {values}"
