from pyflink.datastream.functions import ProcessWindowFunction
from datetime import datetime, timezone
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
        start = datetime.fromtimestamp(context.window().start / 1000, tz=timezone.utc).date()
        end = datetime.fromtimestamp(context.window().end / 1000, tz=timezone.utc).date()

        # Zwróć pojedynczy wynik
        yield f"Window: {start}-{end}; Key: {key} values: {values}"
