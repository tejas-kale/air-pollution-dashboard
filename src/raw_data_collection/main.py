"""
Main script for air pollution data collection.

This script orchestrates the collection of air pollution data for all configured cities.
"""

from datetime import datetime

import yaml

from .air_pollution_collector import AirPollutionCollector


def collect_all_cities(write_mode="append"):
    """
    Collect data for all configured cities.

    Args:
        write_mode (str): Either 'append' or 'overwrite'
    """
    # Initialize collector
    collector = AirPollutionCollector()

    try:
        # Load city list from configuration file
        with open("config/cities.yml", "r", encoding="utf-8") as f:
            cities_config = yaml.safe_load(f)["cities"]

        print(f"Starting data collection at {datetime.now()}")
        print(f"Found {len(cities_config)} cities in configuration")

        # Collect data for each city
        for i, city_data in enumerate(cities_config, 1):
            city_name = city_data["name"]
            print(f"\nProcessing city {i}/{len(cities_config)}: {city_name}")
            collector.collect_data(city_name, write_mode)

        print(f"\nCompleted data collection at {datetime.now()}")

    except FileNotFoundError:
        print("Error: cities.yml configuration file not found")
    except yaml.YAMLError:
        print("Error: Invalid YAML format in cities.yml")
    except Exception as e:
        print(f"Error during data collection: {str(e)}")


if __name__ == "__main__":
    # Run collection for all cities in append mode
    collect_all_cities("append")
