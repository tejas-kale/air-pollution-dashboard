"""
Air Pollution Data Collection Module

This module provides functionality to collect historical air pollution data from the OpenWeatherMap API
and store it in BigQuery. It includes features for automatic date range handling,
data aggregation, and maintaining a record of last updates.
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests
from geopy.geocoders import Nominatim
from google.cloud import bigquery

from src.utils.bq_utils import BQ_CONFIG, load_environment


class AirPollutionCollector:
    """
    A class to collect and store historical air pollution data for cities.

    This class handles the entire process of fetching air pollution data from OpenWeatherMap API,
    including city geocoding, API requests, data parsing, and database storage.

    Attributes:
        api_key (str): OpenWeatherMap API key loaded from environment variables
        base_url (str): Base URL for the OpenWeatherMap API
    """

    def __init__(self):
        """Initialize the collector with configuration."""
        # Load environment variables
        load_environment()
        # Get API key from environment
        self.api_key = os.getenv("OPENWEATHERMAP_API_KEY")
        # Set the API endpoint for historical air pollution data
        self.base_url = "http://api.openweathermap.org/data/2.5/air_pollution/history"

    def get_coordinates(self, city):
        """
        Get latitude and longitude coordinates for a city using geocoding.

        Args:
            city (str): Name of the city

        Returns:
            tuple: (latitude, longitude) coordinates

        Raises:
            ValueError: If city cannot be found
        """
        # Initialize geocoder with our app's user agent
        geolocator = Nominatim(user_agent="air_pollution_app")

        try:
            # Attempt to geocode the city name
            location = geolocator.geocode(city)
            if location:
                return location.latitude, location.longitude
            # Raise error if city not found
            raise ValueError(f"Could not find coordinates for {city}")
        except Exception as e:
            raise ValueError(f"Error getting coordinates: {str(e)}")

    def get_pollution_data(self, lat, lon, start_date, end_date):
        """
        Fetch historical pollution data from OpenWeatherMap API.

        Args:
            lat (float): Latitude of the location
            lon (float): Longitude of the location
            start_date (datetime): Start date
            end_date (datetime): End date

        Returns:
            dict: JSON response from the API containing pollution data

        Raises:
            Exception: If there's an error fetching data from the API
        """
        # Convert datetime objects to Unix timestamps for the API
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())

        # Construct API parameters
        params = {
            "lat": lat,
            "lon": lon,
            "start": start_timestamp,
            "end": end_timestamp,
            "appid": self.api_key,
        }

        try:
            # Make API request with timeout
            response = requests.get(self.base_url, params=params, timeout=300)
            response.raise_for_status()  # Raise exception for bad status codes
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
            pandas.DataFrame: Parsed pollution data
        """
        # Initialize list to store parsed records
        records = []

        # Extract each measurement from the API response
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

        # Convert records to DataFrame
        return pd.DataFrame(records)

    def check_existing_records(self, client, table_ref, city, start_date, end_date):
        """
        Check for existing records in the given date range.

        Args:
            client (bigquery.Client): BigQuery client
            table_ref (str): Full table reference
            city (str): City name
            start_date (datetime): Start date
            end_date (datetime): End date

        Returns:
            set: Set of (city, timestamp) tuples for existing records
        """
        # Query to get existing records
        query = f"""
            SELECT city, timestamp
            FROM `{table_ref}`
            WHERE city = @city
            AND timestamp BETWEEN @start_date AND @end_date
        """

        # Configure query parameters
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("city", "STRING", city),
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            ]
        )

        # Execute query and convert results to set of tuples
        df = client.query(query, job_config=job_config).to_dataframe()
        return set(zip(df["city"], df["timestamp"]))

    def save_to_database(self, df, write_mode="append"):
        """
        Save pollution data to BigQuery, skipping existing records.

        Args:
            df (pandas.DataFrame): DataFrame containing pollution data
            write_mode (str): Either 'append' or 'overwrite'

        Raises:
            Exception: If there's an error saving to BigQuery
        """
        try:
            # Create schema from configuration
            schema = []
            for field in BQ_CONFIG["table"]["schema"]:
                schema.append(
                    bigquery.SchemaField(
                        name=field["name"],
                        field_type=field["type"],
                        mode=field.get("mode", "NULLABLE"),
                        description=field.get("description", ""),
                    )
                )

            # Configure job for data loading
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=(
                    bigquery.WriteDisposition.WRITE_TRUNCATE
                    if write_mode == "overwrite"
                    else bigquery.WriteDisposition.WRITE_APPEND
                ),
            )

            # Initialize BigQuery client
            client = bigquery.Client()
            dataset_ref = client.dataset(BQ_CONFIG["dataset"]["id"])
            table_ref = dataset_ref.table(BQ_CONFIG["table"]["id"])

            if write_mode == "append":
                # Get existing records for the time period
                existing_records = self.check_existing_records(
                    client,
                    f"{client.project}.{dataset_ref.dataset_id}.{table_ref.table_id}",
                    df["city"].iloc[0],  # Assuming all records are for same city
                    df["timestamp"].min(),
                    df["timestamp"].max(),
                )

                # Filter out existing records
                df = df[
                    ~df.apply(
                        lambda row: (row["city"], row["timestamp"]) in existing_records,
                        axis=1,
                    )
                ]

                if df.empty:
                    print("All records already exist in database")
                    return

            print(df)
            # Load filtered data to BigQuery
            # load_job = client.load_table_from_dataframe(
            #     df, table_ref, job_config=job_config
            # )
            # # Wait for job completion
            # load_job.result()

        except Exception as e:
            raise Exception(f"BigQuery error: {str(e)}")

    def collect_data(
        self,
        city: str,
        write_mode: str = "append",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ):
        """
        Collect and store pollution data for a city.

        Args:
            city (str): Name of the city
            write_mode (str): Either 'append' or 'overwrite'
            start_date (datetime, optional): Start date for data collection
                If not provided, defaults to 7 days ago
            end_date (datetime, optional): End date for data collection
                If not provided, defaults to current date

        Raises:
            Exceptions are caught and logged within the method
        """
        try:
            # Calculate date range - use provided dates or default to last week
            if end_date is None:
                end_date = datetime.now()
            if start_date is None:
                start_date = end_date - timedelta(days=7)

            # Get city coordinates
            lat, lon = self.get_coordinates(city)

            # Fetch pollution data from API
            pollution_data = self.get_pollution_data(lat, lon, start_date, end_date)

            # Parse API response into DataFrame
            df = self.parse_pollution_data(pollution_data, city)

            # Check if we got any data
            if df.empty:
                print(f"No data collected for {city}")
                return

            # Save data to BigQuery
            self.save_to_database(df, write_mode)

            print(f"Successfully collected and stored {len(df)} records for {city}")
            print(f"Date range: {start_date} to {end_date}")

        except Exception as e:
            print(f"Error collecting data for {city}: {str(e)}")
