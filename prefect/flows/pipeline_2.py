from prefect import task, flow
from etl.fetch_data import fetch_and_save_movie_data
from etl.update_data import update_reviews 
from etl.transform import MongoDataExtractor  
from etl.load_data import load_data_to_postgres  
import pymongo
import pandas as pd
import os
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
 
logging.basicConfig(level=logging.INFO)
load_dotenv()

# Connect to MongoDB and TMDB API
def connect_mongodb_and_tmdb_api():
    mongo_uri = os.getenv('MONGO_URI')
    client = pymongo.MongoClient(mongo_uri)
    db_name = os.getenv('MONGODB_DATABASE', 'default_db_name').replace(' ', '_')
    db = client[db_name]
    
    tmdb_api_key = os.getenv('TMDB_API_KEY')
    return db, tmdb_api_key

# Task to fetch reviews and load movies in a week
@task(retries=2)
def extract_and_load_recent_movies(release_date_from, release_date_to):
    fetch_and_save_movie_data(release_date_from, release_date_to)

# Task to check the existence of the top popular movies collection
@task
def update_movie_reviews(db, tmdb_api_key, release_date_from, release_date_to):
    update_reviews(db, tmdb_api_key, release_date_from, release_date_to)

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

@flow(name="ETL-pipeline", log_prints=True)
def movie_etl_pipeline():
    # Connect to MongoDB and TMDB API
    db, tmdb_api_key=connect_mongodb_and_tmdb_api()
    
    release_date_from = (datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d')
    release_date_to = datetime.now().strftime('%Y-%m-%d')

    extract_and_load_recent_movies(release_date_from, release_date_to)
    update_movie_reviews(db, tmdb_api_key, release_date_from, release_date_to)
    transformed_data = transform_data()
    load_data(transformed_data)

if __name__ == "__main__":
    """Main ETL pipeline for movie data"""
    movie_etl_pipeline.serve(name="Movie ETL Pipeline",
                            tags=["pipeline2"])
