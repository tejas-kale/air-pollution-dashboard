"""
BigQuery Dataset Creation Script

This script creates a BigQuery dataset for storing air pollution data.
"""

from google.cloud import bigquery
from src.utils.bq_utils import BQ_CONFIG, load_environment

def create_schema_from_config(config):
    """Create BigQuery schema from configuration."""
    schema = []
    for field in config['table']['schema']:
        schema.append(
            bigquery.SchemaField(
                name=field['name'],
                field_type=field['type'],
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description', '')
            )
        )
    return schema

def create_bigquery_dataset(config):
    """
    Creates a BigQuery dataset if it doesn't exist.
    
    Args:
        config (dict): Configuration dictionary
        
    Returns:
        google.cloud.bigquery.dataset.Dataset: The created or existing dataset
        
    Raises:
        Exception: If there's an error creating the dataset
    """
    try:
        # Initialize the BigQuery client
        client = bigquery.Client(project=config['project']['id'])
        
        # Construct the full dataset reference
        dataset_ref = f"{config['project']['id']}.{config['dataset']['id']}"
        
        # Create dataset if it doesn't exist
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = config['project']['location']
        
        # Make an API request to create the dataset
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {dataset_ref} created or already exists.")
        
        # Create schema from configuration
        schema = create_schema_from_config(config)
        
        # Create the air pollution table with schema
        table_id = f"{dataset_ref}.{config['table']['id']}"
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
        print(f"Table {table_id} created or already exists.")
        
        return dataset
        
    except Exception as e:
        raise Exception(f"Error creating BigQuery dataset: {str(e)}")

def main():
    """Main function to create the BigQuery dataset and table."""
    try:
        # Load and validate environment variables
        load_environment()
        
        # Create dataset and table
        create_bigquery_dataset(BQ_CONFIG)
        print("BigQuery setup completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
