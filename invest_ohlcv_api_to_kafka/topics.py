import yaml
import os
from rich.console import Console
import config
import time

console = Console()


class Topic:

    def __init__(self):
        self.topics_list = self.get_topics_from_file()

    @staticmethod
    def get_topics_from_file(self):
        selected_topics_config_file = None
        topics_config_file = "/etc/config/INVEST-TOPICS"
        topics_default_file = "default_topics.yaml"
        while not selected_topics_config_file:
            if not os.path.isfile(topics_config_file):
                console.print(f"{topics_config_file} not found.")
                console.print(f"Trying the local {topics_default_file} file...")
                if not os.path.isfile(topics_default_file):
                    console.print(f"Local file {topics_default_file} not found - unable to continue.", style="yellow")
                    console.print(f"Sleeping for {config.RETRY_TIME.FileNotFound} seconds...")
                    time.sleep(config.RETRY_TIME.FileNotFound)
                else:
                    selected_topics_config_file = topics_default_file
            else:
                selected_topics_config_file = topics_config_file

        valid_config_file = False
        console.print(f"Parsing YAML on file {selected_topics_config_file}...")
        while not valid_config_file:
            try:
                with open(selected_topics_config_file, "r") as yaml_topics_file:
                    try:
                        all_topics = yaml.safe_load(yaml_topics_file)
                        valid_config_file = True
                        return all_topics["invest-ohlcv-topics"]
                    except yaml.YAMLError as ye:
                        console.print(f"ERROR parsing topics YAML file: {selected_topics_config_file}")
                        console.print(ye)
                        console.log(f"Sleeping for {config.RETRY_TIME.ConfigurationError} seconds...")
                        time.sleep(config.RETRY_TIME.ConfigurationError)
            except Exception as ex:
                console.print(f"ERROR opening {selected_topics_config_file} file.")
                console.print(ex)
