"""
Air Pollution Data Collection Module

This module provides functionality to collect historical air pollution data from the OpenWeatherMap API
and store it in BigQuery.
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import pytz
import requests
from geopy.geocoders import Nominatim
from google.cloud import bigquery
from timezonefinder import TimezoneFinder

from src.utils.bq_utils import BQ_CONFIG, load_environment


class AirPollutionCollector:
    """A class to collect and store historical air pollution data for cities."""

    def __init__(self):
        """Initialize the collector with configuration."""
        load_environment()
        self.api_key = os.getenv("OPENWEATHERMAP_API_KEY")
        self.base_url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
        self.tf = TimezoneFinder()  # Initialize TimezoneFinder

    def get_coordinates(self, city):
        """Get latitude and longitude coordinates for a city using geocoding."""
        # Initialize geocoder with our app's user agent
        geolocator = Nominatim(user_agent="air_pollution_app")

        try:
            # Attempt to geocode the city name
            location = geolocator.geocode(city)
            if location:
                return location.latitude, location.longitude
            raise ValueError(f"Could not find coordinates for {city}")
        except Exception as e:
            raise ValueError(f"Error getting coordinates: {str(e)}")

    def get_timezone(self, lat: float, lon: float) -> pytz.timezone:
        """
        Get timezone for given coordinates.

        Args:
            lat (float): Latitude
            lon (float): Longitude

        Returns:
            pytz.timezone: Timezone object for the location
        """
        # Get timezone string for coordinates
        timezone_str = self.tf.timezone_at(lat=lat, lng=lon)
        if not timezone_str:
            # Default to UTC if timezone not found
            print(f"Warning: Could not find timezone for coordinates ({lat}, {lon}). Using UTC.")
            return pytz.UTC
        
        return pytz.timezone(timezone_str)

    def convert_timestamp(self, timestamp: int, timezone: pytz.timezone) -> datetime:
        """
        Convert Unix timestamp to datetime in specified timezone.

        Args:
            timestamp (int): Unix timestamp in UTC
            timezone (pytz.timezone): Target timezone

        Returns:
            datetime: Localized datetime object
        """
        # Convert Unix timestamp to UTC datetime
        utc_dt = datetime.fromtimestamp(timestamp, pytz.UTC)
        # Convert to target timezone
        return utc_dt.astimezone(timezone)

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

    def parse_pollution_data(self, data: dict, city: str, timezone: pytz.timezone) -> pd.DataFrame:
        """
        Parse API response data into a pandas DataFrame and ensure hourly completeness.

        Args:
            data (dict): Raw API response data
            city (str): City name to include in the records
            timezone (pytz.timezone): Timezone for the city

        Returns:
            pandas.DataFrame: Parsed pollution data with complete hourly timestamps
        """
        records = []

        # Parse API response
        for item in data["list"]:
            local_dt = self.convert_timestamp(item["dt"], timezone)
            record = {
                "city": city,
                "timestamp": local_dt,
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

        # Convert to DataFrame
        df = pd.DataFrame(records)
        if df.empty:
            return df

        # Generate complete hourly sequence
        min_time = df["timestamp"].min()
        max_time = df["timestamp"].max()
        complete_hours = pd.date_range(
            start=min_time,
            end=max_time,
            freq="h",
            tz=timezone
        )

        # Create template DataFrame with all hours
        template = pd.DataFrame({
            "timestamp": complete_hours,
            "city": city
        })

        # Merge with actual data
        df = pd.merge(
            template,
            df,
            on=["city", "timestamp"],
            how="left"
        )

        # Initialize numeric columns with explicit null values
        numeric_columns = ["aqi", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]
        for col in numeric_columns:
            if col not in df.columns:
                df[col] = None
            else:
                df[col] = df[col].astype('float64')
                df[col] = df[col].where(df[col].notna(), None)

        return df

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
        Save pollution data to BigQuery, handling overwrites per city.

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

            # Initialize BigQuery client
            client = bigquery.Client()
            dataset_ref = client.dataset(BQ_CONFIG["dataset"]["id"])
            table_ref = dataset_ref.table(BQ_CONFIG["table"]["id"])
            table_id = f"{client.project}.{dataset_ref.dataset_id}.{table_ref.table_id}"

            if write_mode == "overwrite":
                # Get the city we're processing
                city = df["city"].iloc[0]
                
                # Create temporary table for new data
                temp_table_id = f"{table_id}_temp"
                
                # Load new data to temporary table
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                )
                load_job = client.load_table_from_dataframe(
                    df, temp_table_id, job_config=job_config
                )
                load_job.result()

                # Merge data, replacing only records for the current city
                merge_query = f"""
                    CREATE OR REPLACE TABLE `{table_id}` AS
                    SELECT * FROM `{table_id}`
                    WHERE city != @city
                    UNION ALL
                    SELECT * FROM `{temp_table_id}`
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("city", "STRING", city)
                    ]
                )
                
                merge_job = client.query(merge_query, job_config=job_config)
                merge_job.result()

                # Clean up temporary table
                client.delete_table(temp_table_id)

            else:  # append mode
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )

                # Get existing records for the time period
                existing_records = self.check_existing_records(
                    client,
                    table_id,
                    df["city"].iloc[0],
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

                # Load filtered data to BigQuery
                load_job = client.load_table_from_dataframe(
                    df, table_ref, job_config=job_config
                )
                load_job.result()

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
            end_date (datetime, optional): End date for data collection
        """
        try:
            # Calculate date range
            if end_date is None:
                end_date = datetime.now()
            if start_date is None:
                start_date = end_date - timedelta(days=7)

            # Get city coordinates and timezone
            lat, lon = self.get_coordinates(city)
            timezone = self.get_timezone(lat, lon)

            # Convert dates to UTC for API request
            start_utc = start_date.astimezone(pytz.UTC)
            end_utc = end_date.astimezone(pytz.UTC)

            # Fetch pollution data from API
            pollution_data = self.get_pollution_data(lat, lon, start_utc, end_utc)

            # Parse API response into DataFrame with city's timezone
            df = self.parse_pollution_data(pollution_data, city, timezone)

            if df.empty:
                print(f"No data collected for {city}")
                return

            # Save data to BigQuery
            self.save_to_database(df, write_mode)

            print(f"Successfully collected and stored {len(df)} records for {city}")
            print(f"Date range: {start_date} to {end_date} ({timezone})")

        except Exception as e:
            print(f"Error collecting data for {city}: {str(e)}")
