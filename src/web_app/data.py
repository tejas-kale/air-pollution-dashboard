"""Data fetching module for the Streamlit app."""

import streamlit as st
from google.cloud import bigquery
from src.utils.bq_utils import BQ_CONFIG

@st.cache_data(ttl=3600)
def get_annual_means():
    """Fetch annual mean data from BigQuery."""
    client = bigquery.Client(project=BQ_CONFIG["project"]["id"])
    query = """
    SELECT *
    FROM `air_pollution_staging.stg_annual_mean`
    ORDER BY city, year
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def get_rolling_means(start_date, end_date):
    """Fetch rolling mean data for the specified date range."""
    client = bigquery.Client(project=BQ_CONFIG["project"]["id"])
    query = """
    SELECT *
    FROM `air_pollution_staging.stg_rolling_24h_mean`
    WHERE timestamp BETWEEN @start_date AND @end_date
    ORDER BY city, timestamp
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
            bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()

@st.cache_data(ttl=3600)
def get_o3_peak_season():
    """Fetch ozone peak season data."""
    client = bigquery.Client(project=BQ_CONFIG["project"]["id"])
    query = """
    SELECT *
    FROM `air_pollution_staging.stg_o3_peak_season`
    ORDER BY city, date
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def get_o3_rolling():
    """Fetch ozone 8-hour rolling max data."""
    client = bigquery.Client(project=BQ_CONFIG["project"]["id"])
    query = """
    SELECT *
    FROM `air_pollution_staging.stg_o3_8h_rolling`
    ORDER BY city, date
    """
    return client.query(query).to_dataframe() 