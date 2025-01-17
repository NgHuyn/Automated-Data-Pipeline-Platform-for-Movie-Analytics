from prefect import task, flow, serve
from prefect.client.schemas.schedules import IntervalSchedule
from etl.fetch_data import fetch_and_save_movie_data
from etl.update_data import update_reviews 
from etl.transform import MongoDataExtractor  
from etl.load_data import load_data_to_postgres  
import pymongo
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

# Connect to MongoDB and TMDB API
def connect_mongodb_and_tmdb_api():
    mongo_uri = os.getenv('MONGO_URI')
    client = pymongo.MongoClient(mongo_uri)
    db_name = os.getenv('MONGODB_DATABASE', 'default_db_name').replace(' ', '_')
    db = client[db_name]
    
    tmdb_api_key = os.getenv('TMDB_API_KEY')
    return db, tmdb_api_key

@task(retries=2)
def fetch_movie_data(release_date_from, release_date_to):
    """Fetch movie data and save it to MongoDB."""
    fetch_and_save_movie_data(release_date_from, release_date_to)

@task(retries=2)
def update_movie_reviews(release_date_from, release_date_to):
    """Check the existence of the top popular movies collection"""
    db, tmdb_api_key=connect_mongodb_and_tmdb_api()
    update_reviews(db, tmdb_api_key, release_date_from, release_date_to)

@task(retries=2)
def transform_data():
    """Transform data from MongoDB into a format suitable for PostgreSQL."""
    extractor = MongoDataExtractor()
    transformed_data = extractor.process_all_collections()
    return transformed_data

@task(retries=2)
def load_data(transformed_data):
    """Load transformed data into PostgreSQL."""
    for table_name, data in transformed_data.items():
        if isinstance(data, pd.DataFrame) and not data.empty:
            load_data_to_postgres(data, table_name)

@flow(name="manually-ETL-pipeline", log_prints=True)
def manually_etl_pipeline(release_date_from, release_date_to):
    fetch_movie_data(release_date_from, release_date_to)
    transformed_data = transform_data()
    load_data(transformed_data)

@flow(name="ETL-pipeline", log_prints=True)
def movie_etl_pipeline():
    release_date_from = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
    release_date_to = datetime.now().strftime('%Y-%m-%d')

    fetch_movie_data(release_date_from, release_date_to)
    update_movie_reviews(release_date_from, release_date_to)
    transformed_data = transform_data()
    load_data(transformed_data)

if __name__ == "__main__":
    """Main ETL pipeline for movie data"""
    pipeline_1 = manually_etl_pipeline.to_deployment(name="Manually ETL Pipeline",
                                                     tags=["pipeline1"],
                                                     parameters={"release_date_from": '2024-01-01', "release_date_to": '2024-01-02'})
    # Get time for schedule
    anchor_date_str = os.getenv("ANCHOR_DATE", "2024-11-29 10:00:00")  
    timezone_str = os.getenv("TIMEZONE", "Asia/Saigon")
    timezone_tz = ZoneInfo(timezone_str)

    try:
        # Convert anchor_date into datetime & apply timezone
        anchor_date = datetime.strptime(anchor_date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone_tz)
    except ValueError as e:
        logging.error(f"Invalid ANCHOR_DATE format: {e}")
        raise ValueError("ANCHOR_DATE must be in 'YYYY-MM-DD HH:MM:SS' format")

    pipeline_2 = movie_etl_pipeline.to_deployment(
        name="Movie ETL Pipeline",
        tags=["pipeline2"],
        schedules=[
            IntervalSchedule(
                interval=timedelta(days=7),
                anchor_date=anchor_date  
            )
        ]
    )
    
    serve(pipeline_1, pipeline_2)