"""Downloads historical data for a list of tickers."""

import os
import time
import uuid
from kafka import KafkaProducer, errors
import orjson
import yaml
from icecream import ic
from datetime import datetime
import redis
from rich.console import Console
from typing import Union, List, Dict, Any
import config
from topics import Topic
import investpy
import pandas as pd

console = Console()


class InvestingData:

    def __init__(self, products: List[str], countries: Union[List[str], None] = None):
        if countries is None:
            self.countries = ["united states"]
        else:
            self.countries = countries
        self.products = products
        self.symbol = None
        self.exchange = None

    def __search(self, symbol: str):
        try:
            search = investpy.search_quotes(
                text=symbol,
                products=self.products,
                countries=self.countries,
                n_results=1
            )
            return search
        except ConnectionError as re:
            console.log("Connection error connecting to Investing.com:")
            console.log(re)
        except ValueError as ve:
            console.log("Value error: wrong parameter value for search.")
            console.log(ve)
        return None

    def retrieve_historical_data(self, symbol, start_date, end_date) -> List[Dict[str, Any]]:
        self.symbol = symbol
        search_obj = self.__search(symbol)
        if not search_obj:
            console.log(f"FAILED acquiring search object for symbol {symbol}")
            return []
        self.exchange = search_obj.exchange
        try:
            historical_data_json = search_obj.retrieve_historical_data(
                self.convert_date_to_pt(start_date),
                self.convert_date_to_pt(end_date)
            ).reset_index().to_json(orient="records", date_format="iso")
            return orjson.loads(historical_data_json)
        except RuntimeError as re:
            console.log(f"Failed retrieving historical data for symbol {symbol} from investing.com")
            console.log(re)
        except ValueError as ve:
            console.log(f"Invalid parameters to retrieve historical. (symbol:{symbol})")
            console.log(ve)
        return []

    @staticmethod
    def convert_date_to_pt(iso_date: str) -> str:
        dt_obj = datetime.strptime(iso_date, "%Y-%m-%d")
        return dt_obj.strftime("%d/%m/%Y")


class InvestingAPItoKafka:

    def __init__(self):
        self.topic: str = "invest.events.ohlcv.queue"
        self.kafka_producer = None
        self.stocks_list: Union[List[str], None] = None
        self.etfs_list: Union[List[str], None] = None
        self.indices_list: Union[List[str], None] = None
        self.get_symbols()

    def get_symbols(self) -> None:
        self.get_symbols_list_from_redis()
        if (
                not self.stocks_list or len(self.stocks_list) == 0
        ) and (
                not self.etfs_list or len(self.etfs_list) == 0
        ) and (
                not self.indices_list or len(self.indices_list) == 0
        ):
            self.get_symbols_list_from_file()

    def get_symbols_list_from_redis(self) -> None:
        try:
            r = redis.Redis(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                db=config.REDIS_DB,
                username=config.REDIS_USERNAME if config.REDIS_HOST != "localhost" else None,
                password=config.REDIS_PASSWORD if config.REDIS_HOST != "localhost" else None
            )
            self.stocks_list = r.smembers("INVEST-STOCKS")
            self.etfs_list = r.smembers("INVEST-ETFS")
            self.indices_list = r.smembers("INVEST-INDICES")
        except redis.exceptions.ConnectionError as ce:
            console.log("REDIS connection error:")
            console.log(ce)

    def get_symbols_list_from_file(self) -> None:
        selected_symbols_config_file: Union[str, None] = None
        symbols_config_file = "/etc/config/INVEST"
        symbols_local_file = "default_tickers.yaml"
        while not selected_symbols_config_file:
            if not os.path.isfile(symbols_config_file):
                console.print(f"{symbols_config_file} not found.")
                console.print("Trying with the local file:")
                if not os.path.isfile(symbols_local_file):
                    console.print(f"Local file {symbols_local_file} not found.", style="yellow")
                    console.print("No symbols configuration files found - cannot continue.")
                    console.log(f"Sleeping for {config.RETRY_TIME.FileNotFound} seconds...")
                    time.sleep(config.RETRY_TIME.FileNotFound)
                else:
                    selected_symbols_config_file = symbols_local_file
            else:
                selected_symbols_config_file = symbols_config_file
        valid_config_file = False
        console.print(f"Parsing {selected_symbols_config_file}...")
        while not valid_config_file:
            with open(selected_symbols_config_file, "r") as yaml_symbols_file:
                try:
                    all_symbols = yaml.safe_load(yaml_symbols_file)
                    valid_config_file = True
                    self.stocks_list = all_symbols["invest-stocks"]
                    self.etfs_list = all_symbols["invest-etfs"]
                    self.indices_list = all_symbols["invest-indices"]
                    ic(all_symbols)
                except yaml.YAMLError as ye:
                    console.print(f"ERROR parsing the yaml file {selected_symbols_config_file}", style="bold yellow")
                    console.print(ye)
                    console.log(f"Sleeping for {config.RETRY_TIME.ConfigurationError} seconds.")
                    time.sleep(config.RETRY_TIME.ConfigurationError)

    def retrieve_data(self, start_date: str, end_date: str):
        # Stocks
        stocks = InvestingData(products=["stocks"])
        ic(self.stocks_list)
        for ticker in self.stocks_list:
            ticker_only = ticker.split(":")[1]
            console.log(f"Retrieving hist data for symbol {ticker_only}")
            stock_data_list = stocks.retrieve_historical_data(symbol=ticker_only, start_date=start_date, end_date=end_date)
            self.publish_data(symbol=ticker_only, exchange=stocks.exchange, datapoints=stock_data_list)

    def publish_data(self, symbol: str, exchange: str, datapoints: List[Dict[str, Any]]) -> None:
        if not datapoints or len(datapoints) == 0:
            console.log(f"Warning! No datapoint for symbol {symbol} - nothing to publish.")
            return None
        topics = Topic()
        if not self.kafka_producer:
            self.open_kafka_connection()
        for bar in datapoints:
            bar["id"] = uuid.uuid4()
            bar["provider"] = "INVEST"
            bar["timeframe"] = "day"
            bar["symbol"] = symbol
            bar["price_date"] = datetime.strptime(bar["Date"].split("T")[0], '%Y-%m-%d')
            del bar["Date"]
            bar["exchange"] = exchange
            bar["open"] = bar["Open"]
            del bar["Open"]
            bar["high"] = bar["High"]
            del bar["High"]
            bar["low"] = bar["Low"]
            del bar["Low"]
            bar["close"] = bar["Close"]
            del bar["Close"]
            bar["volume"] = bar["Volume"]
            del bar["Volume"]
            del bar["Change Pct"]
            bar["u_open"] = None
            bar["u_high"] = None
            bar["u_low"] = None
            bar["u_close"] = None
            bar["u_volume"] = None
            bar["vwap"] = None
            bar["trade_count"] = None
            bar["currency"] = "USD"
            bar["dividend_amount"] = None
            bar["split_coefficient"] = None
            bar["alt_symbol"] = None
            ic(bar)
            for topic_name in topics.topics_list:
                console.print(f"Publishing to {topic_name}, symbol:{symbol}, datapoint: {bar['price_date']}")
                self.kafka_producer.send(topic_name, value=bar)
        self.kafka_producer.flush()

    def open_kafka_connection(self):
        while not self.kafka_producer:
            try:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: orjson.dumps(v),
                )
                console.print(f"Connection opened to Kafka on {config.KAFKA_BOOTSTRAP_SERVERS}", style="green")
            except errors.NoBrokersAvailable as ke_nba:
                console.log(f"FAILED to connect to Kafka brokers: {config.KAFKA_BOOTSTRAP_SERVERS}")
                console.log(ke_nba)
                console.log(f"Sleeping for {config.RETRY_TIME.ServiceUnavailable} seconds...")
                time.sleep(config.RETRY_TIME.ServiceUnavailable)


if __name__ == "__main__":
    invest = InvestingAPItoKafka()
    ic(invest.stocks_list)
    ic(invest.indices_list)
    ic(invest.topic)
    #stocks = InvestingData(products=["stocks"])
    # stock_data = stocks.retrieve_historical_data(
    #     symbol="TSLA",
    #     start_date="2022-03-01",
    #     end_date="2022-03-08"
    # )
    # ic(stock_data)
    # ic(type(stock_data))

    invest.retrieve_data(start_date="2022-02-01", end_date="2022-02-05")
    # console.print(f"symbol: {stocks.symbol}")





