import os
import argparse
import configparser

from pyflink.common import WatermarkStrategy, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types

from connectors.taxi_event_source import TaxiEventSourceFunction
from tools.enrich_with_loc_data import EnrichWithLocData
from tools.taxi_loc_aggregator import TaxiLocAggregator
from tools.get_final_result_window_function import GetFinalResultWindowFunction

from models.taxi_event import TAXI_EVENT_TYPE, TaxiEvent


def main():
    # 1) Parsowanie ścieżki do pliku konfiguracyjnego
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default="flink.properties",
        help="Ścieżka do pliku .properties"
    )
    args = parser.parse_args()

    # 2) Wczytaj ustawienia
    cfg = configparser.ConfigParser()
    cfg.read(args.config)
    taxi_dir      = cfg["taxiEvents"]["directoryPath"]
    delay_ms      = int(cfg["taxiEvents"]["elementDelayMillis"])
    max_elements  = int(cfg["taxiEvents"]["maxElements"])
    loc_file_path = cfg["locFile"]["path"]

    jar_path = cfg['javaConnectors']['taxiEventSourceJar']

    # 3) Utwórz środowisko Flinka
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(jar_path)

    env.set_parallelism(1)

    # TODO: UWAGA! Zanim rozpoczniesz implementację
    #       - zapoznaj się z celem przetwarzania
    #       - zapoznaj się zaimplementowanymi już komponentami, a także plikiem parametrów
    #       - postaraj się określić docelową rolę komponentów (zrozumieć cel ich implementacji)

    # 4) Źródło z plików CSV

    taxi_src = TaxiEventSourceFunction(taxi_dir, delay_ms, max_elements)

    # TODO: Utwórz źródłowy strumień. Skorzystaj z powyżej utworzonego obiektu TaxiEventSourceFunction.
    #       Zadbaj o znaczniki czasowe oraz znaczniki watermark.
    #       Załóż, że dane są uporządkowane względem znaczników czasowych
    #       TaxiEventSourceFunction daje w wyniku DataStream<Row>. Zamień go na DataStream<TaxiEvent>

    # taxi_events =


    # 5) Wzbogacanie o informacje lokalizacyjne

    # TODO: W strumieniu źródłowym brak jest informacji na temat dzielnicy,
    #       jest ona dostępna w oddzielnym statycznym zbiorze danych.
    #       Uzupełnij dane w strumieniu o nazwę dzielnicy.
    #       Przy okazji pozbądź się danych nieistotnych z punktu widzenia celu przetwarzania
    #       Wynikiem powinien być DataStream<TaxiLocEvent>

    # taxi_loc_events =

    # 6) Określenie klucza, utworzenie okna i agregacja

    # TODO: Mamy już komplet potrzebnych informacji do wykonania naszych obliczeń.
    #       Twoim celem jest dokonywanie obliczeń dla
    #       - każdej dzielnicy i
    #       - każdego kolejnego dnia.
    #       Chcemy dowiedzieć się:
    #       - ile było wyjazdów (startStop = 0),
    #       - ile było przyjazdów (startStop = 1)
    #       - jaka liczba pasażerów została obsłużona (tylko dla przyjazdów)
    #       - jaka sumaryczna kwota została uiszczona za przejazdy (tylko dla przyjazdów)
    #       Wynikiem ma być DataStream<ResultData>

    # result_stream =

    # 7) Wypisz wyniki na stdout

    # TODO: Podłącz ujście. Wystarczy, że będziesz generował na dane na standardowe wyjście.

    # result_stream.print()

    # 8) Uruchom job
    env.execute("Taxi Events Analysis")


if __name__ == "__main__":
    main()
