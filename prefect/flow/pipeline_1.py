from prefect import task, flow
from dotenv import load_dotenv
import os
import pandas as pd
import logging
from movie_crawling.fetch_data import fetch_and_save_movie_data  
from etl.load_data import load_data_to_postgres  
from etl.transform import MongoDataExtractor  

load_dotenv()
logging.basicConfig(level=logging.INFO)

@task
def fetch_movie_data(release_date_from, release_date_to):
    """Fetch movie data and save it to MongoDB."""
    fetch_and_save_movie_data(release_date_from, release_date_to)

@task
def transform_data():
    """Transform data from MongoDB into a format suitable for PostgreSQL."""
    extractor = MongoDataExtractor()
    transformed_data = extractor.process_all_collections()
    return transformed_data

@task
def load_data(transformed_data):
    """Load transformed data into PostgreSQL."""
    for table_name, data in transformed_data.items():
        if isinstance(data, pd.DataFrame) and not data.empty:
            load_data_to_postgres(data, table_name)

@flow(name="movie-ETL-pipeline")
def movie_etl_pipeline(release_date_from, release_date_to):
    """Main ETL pipeline for movie data."""
    fetch_movie_data(release_date_from, release_date_to)
    transformed_data = transform_data()
    load_data(transformed_data)

if __name__ == "__main__":
    movie_etl_pipeline("2024-09-02", "2024-09-02")