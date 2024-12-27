"""
BigQuery Utilities Module

This module provides shared functionality for BigQuery operations,
including configuration loading and environment setup.
"""

import os
from pathlib import Path
import yaml
from dotenv import load_dotenv
from google.cloud import bigquery

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()

def load_bq_config():
    """
    Load BigQuery configuration from YAML file.
    
    Returns:
        dict: Configuration dictionary
        
    Raises:
        FileNotFoundError: If bq.yml is not found
    """
    config_path = PROJECT_ROOT / "config" / "bq.yml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"BigQuery config file not found at {config_path}")
        
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def load_environment():
    """
    Load environment variables from .env file.
    
    Returns:
        dict: Dictionary containing required environment variables
    
    Raises:
        FileNotFoundError: If .env file is not found
        ValueError: If required environment variables are missing
    """
    env_path = PROJECT_ROOT / ".env"
    
    if not env_path.exists():
        raise FileNotFoundError(
            f".env file not found at {env_path}. "
            "Please create one using .env.example as a template."
        )
    
    # Load environment variables from .env
    load_dotenv(env_path)
    
    # Check for required environment variables
    required_vars = ['GOOGLE_APPLICATION_CREDENTIALS', 'BIGQUERY_PROJECT_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
    
    return {
        'google_creds': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'openweather_key': os.getenv('OPENWEATHERMAP_API_KEY')
    }

def get_bigquery_client():
    """
    Create and return a BigQuery client using environment configuration.
    """
    load_environment()
    
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    if not project_id:
        raise ValueError("BIGQUERY_PROJECT_ID environment variable is not set")
        
    return bigquery.Client(project=project_id)

# Load configuration once at module level
BQ_CONFIG = load_bq_config()
