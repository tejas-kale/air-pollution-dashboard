"""
Air Pollution Data Collection Script

This script collects historical air pollution data for specified cities using the OpenWeatherMap API
and stores it in BigQuery. It handles multiple cities sequentially with rate limiting
to avoid API throttling.
"""

from pathlib import Path
import time
import yaml

from air_pollution_collector import AirPollutionCollector

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()

def load_cities():
    """
    Load city list from configuration file.
    
    Returns:
        list: List of city names in specified order
    """
    config_path = PROJECT_ROOT / "config" / "cities.yml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return [city["name"] for city in config["cities"]]

def main():
    """Main execution function that orchestrates the data collection process."""
    # Load cities from config
    cities = load_cities()
    
    # Initialize the collector
    project_id = "august-cirrus-399913"
    collector = AirPollutionCollector(project_id)
    
    # Collect data for each city
    for city in cities:
        print(f"\nProcessing {city}...")
        collector.collect_data(city)  # Uses automatic date range
        time.sleep(2)  # Add delay to avoid hitting API rate limits
        
    print("\nData collection completed for all cities!")

if __name__ == "__main__":
    main() 