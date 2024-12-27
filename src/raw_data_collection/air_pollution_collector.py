"""
Air Pollution Data Collection Module

This module provides functionality to collect historical air pollution data from the OpenWeatherMap API
and store it in a SQLite database. It includes features for automatic date range handling,
data aggregation, and maintaining a record of last updates.

The module uses environment variables for API key management and implements error handling
for API requests and database operations.

Classes:
    AirPollutionCollector: Main class for handling air pollution data collection and storage.
"""

import json
import os
from datetime import date, datetime

import pandas as pd
import requests
from geopy.geocoders import Nominatim
from google.cloud import bigquery

from src.utils.bq_utils import BQ_CONFIG, load_environment


class AirPollutionCollector:
    """
    A class to collect and store historical air pollution data for cities.

    This class handles the entire process of fetching air pollution data from OpenWeatherMap API,
    including city geocoding, API requests, data parsing, and database storage. It maintains
    a SQLite database and provides views for daily aggregated data.

    Attributes:
        api_key (str): OpenWeatherMap API key loaded from environment variables
        db_path (str): Path to the SQLite database file
        base_url (str): Base URL for the OpenWeatherMap API
        last_update_file (str): Path to the JSON file storing last update dates
    """

    def __init__(self):
        """Initialize the collector with configuration."""
        env_vars = load_environment()
        self.api_key = env_vars["openweather_key"]
        self.project_id = BQ_CONFIG["project"]["id"]
        self.client = bigquery.Client(project=self.project_id)
        self.base_url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
        self.last_update_file = "last_update.json"

    def _load_last_update(self, city):
        """
        Load the last update date for a city from JSON file.

        Args:
            city (str): Name of the city

        Returns:
            str: Last update date in 'YYYY-MM-DD' format if found, None otherwise

        Note:
            Returns None if the file doesn't exist or if there's an error reading it
        """
        try:
            if os.path.exists(self.last_update_file):
                with open(self.last_update_file, "r", encoding="utf-8") as f:
                    updates = json.load(f)
                    return updates.get(city)
            return None
        except Exception:
            return None

    def _save_last_update(self, city, update_date):
        """
        Save the last update date for a city to JSON file.

        Args:
            city (str): Name of the city
            update_date (str): Update date in 'YYYY-MM-DD' format

        Note:
            Creates the file if it doesn't exist, updates existing entry if it does
        """
        updates = {}
        if os.path.exists(self.last_update_file):
            with open(self.last_update_file, "r", encoding="utf-8") as f:
                updates = json.load(f)

        updates[city] = update_date

        with open(self.last_update_file, "w", encoding="utf-8") as f:
            json.dump(updates, f)

    def get_coordinates(self, city):
        """
        Get latitude and longitude coordinates for a city using geocoding.

        Args:
            city (str): Name of the city

        Returns:
            tuple: (latitude, longitude) coordinates

        Raises:
            ValueError: If city cannot be found
            Exception: If there's an error in geocoding
        """
        # Initialize geocoder with a user-agent
        geolocator = Nominatim(user_agent="air_pollution_app")

        try:
            # Get location data
            location = geolocator.geocode(city)
            if location:
                return location.latitude, location.longitude
            else:
                raise ValueError(f"Could not find coordinates for {city}")
        except Exception as e:
            raise Exception(f"Error getting coordinates: {str(e)}")

    def get_pollution_data(self, lat, lon, start_date, end_date):
        """
        Fetch historical pollution data from OpenWeatherMap API.

        Args:
            lat (float): Latitude of the location
            lon (float): Longitude of the location
            start_date (str): Start date in 'YYYY-MM-DD' format
            end_date (str): End date in 'YYYY-MM-DD' format

        Returns:
            dict: JSON response from the API containing pollution data

        Raises:
            Exception: If there's an error fetching data from the API
        """
        # Convert dates to Unix timestamps
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

        # Construct API URL
        params = {
            "lat": lat,
            "lon": lon,
            "start": start_timestamp,
            "end": end_timestamp,
            "appid": self.api_key,
        }

        try:
            response = requests.get(self.base_url, params=params, timeout=300)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error fetching pollution data: {str(e)}")

    def parse_pollution_data(self, data, city):
        """
        Parse API response data into a pandas DataFrame.

        Args:
            data (dict): Raw API response data
            city (str): City name to include in the records

        Returns:
            pandas.DataFrame: Parsed pollution data with columns for each pollutant

        Note:
            The resulting DataFrame includes columns for timestamp, city, AQI,
            and various pollutant measurements (CO, NO, NO2, etc.)
        """
        records = []

        for item in data["list"]:
            record = {
                "city": city,
                "timestamp": datetime.fromtimestamp(item["dt"]),
                "aqi": item["main"]["aqi"],
                "co": item["components"]["co"],
                "no": item["components"]["no"],
                "no2": item["components"]["no2"],
                "o3": item["components"]["o3"],
                "so2": item["components"]["so2"],
                "pm2_5": item["components"]["pm2_5"],
                "pm10": item["components"]["pm10"],
                "nh3": item["components"]["nh3"],
            }
            records.append(record)

        return pd.DataFrame(records)

    def save_to_database(self, df):
        """
        Save pollution data to BigQuery, handling duplicates.

        Args:
            df (pandas.DataFrame): DataFrame containing pollution data
            
        Raises:
            Exception: If there's an error saving to BigQuery
        """
        try:
            # Get schema from config
            schema = []
            for field in BQ_CONFIG['table']['schema']:
                schema.append(
                    bigquery.SchemaField(
                        name=field['name'],
                        field_type=field['type'],
                        mode=field.get('mode', 'NULLABLE'),
                        description=field.get('description', '')
                    )
                )

            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=schema
            )

            # Get the dataset reference
            dataset_ref = self.client.dataset(BQ_CONFIG['dataset']['id'])
            table_ref = dataset_ref.table(BQ_CONFIG['table']['id'])

            # Load the data
            load_job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            load_job.result()  # Wait for the job to complete

        except Exception as e:
            raise Exception(f"BigQuery error: {str(e)}")

    def collect_data(self, city, start_date=None, end_date=None):
        """
        Collect and store pollution data for a city.

        This is the main method that orchestrates the entire data collection process,
        from fetching coordinates to storing in the database.

        Args:
            city (str): Name of the city
            start_date (str, optional): Start date in 'YYYY-MM-DD' format
            end_date (str, optional): End date in 'YYYY-MM-DD' format

        Note:
            If dates are not specified, uses the last update date (or one month ago)
            as start date and current date as end date

        Raises:
            Exceptions are caught and logged within the method
        """
        try:
            # If dates not specified, use last update date to current date
            if not start_date:
                start_date = self._load_last_update(city)
                if not start_date:
                    # If no last update, start from a month ago
                    start_date = (
                        date.today().replace(day=1) - pd.DateOffset(months=1)
                    ).strftime("%Y-%m-%d")

            if not end_date:
                end_date = date.today().strftime("%Y-%m-%d")

            # Get coordinates
            lat, lon = self.get_coordinates(city)

            # Get pollution data
            pollution_data = self.get_pollution_data(lat, lon, start_date, end_date)

            # Parse data
            df = self.parse_pollution_data(pollution_data, city)

            # Save to database
            self.save_to_database(df)

            # Save last update date
            self._save_last_update(city, end_date)

            print(f"Successfully collected and stored data for {city}")

        except Exception as e:
            print(f"Error collecting data for {city}: {str(e)}")
