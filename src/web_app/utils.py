"""Utility functions for the Streamlit app."""

import yaml
from pathlib import Path

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()

def load_cities():
    """Load city list from configuration file."""
    config_path = PROJECT_ROOT / "config" / "cities.yml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return [city["name"] for city in config["cities"]]

def load_pollutant_config():
    """
    Load pollutant configuration from YAML file.
    
    Returns:
        dict: Pollutant configuration dictionary
    """
    config_path = PROJECT_ROOT / "config" / "pollutants.yml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config["pollutants"] 