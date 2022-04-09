"""Downloads historical data for a list of tickers."""

import os
import time

import yaml
from icecream import ic
import redis
from rich.console import Console
from typing import Union, List, Dict, Any
import config
import investpy

console = Console()


class InvestingData:

    def __init__(self, products: List[str], countries: Union[List[str], None]=None):
        if countries is None:
            self.countries = ["united states"]
        else:
            self.countries = countries
        self.products = products

    def __search(self, symbol: str):
        search = investpy.search_quotes(
            text=symbol,
            products=self.products,
            countries=self.countries,
            n_results=1
        )
        return search

    def retrieve_historical_data(self, symbol, start_date, end_date):
        search_obj = self.__search(symbol)
        start_date_day_first =
        ic(search_obj)
        historical_data: Dict[str, Any] = search_obj.retrieve_historical_data("01/01/2022", "5/01/2022").reset_index().to_dict("records")

    @staticmethod
    def convert_date_to_pt(self, iso_date: str) -> str:
        return " "

class InvestingAPItoKafka:

    def __init__(self):
        self.topic: str = "invest.events.ohlcv.queue"
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
                    self.topic = all_symbols["invest-ohlcv-events-queue"]
                    ic(all_symbols)
                except yaml.YAMLError as ye:
                    console.print(f"ERROR parsing the yaml file {selected_symbols_config_file}", style="bold yellow")
                    console.print(ye)
                    console.log(f"Sleeping for {config.RETRY_TIME.ConfigurationError} seconds.")
                    time.sleep(config.RETRY_TIME.ConfigurationError)


if __name__ == "__main__":
    invest = InvestingAPItoKafka()
    ic(invest.stocks_list)
    ic(invest.indices_list)
    ic(invest.topic)
