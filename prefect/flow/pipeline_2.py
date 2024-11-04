from prefect import task, flow
from movie_crawling.fetch_data import fetch_and_save_movie_data
import pymongo
import os
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
from etl.update_data import update_reviews  


logging.basicConfig(level=logging.INFO)

# Task to fetch reviews and load movies in a week
@task(retries=2)
def extract_and_load_recent_movies(release_date_from, release_date_to):
    fetch_and_save_movie_data(release_date_from, release_date_to)

# Task to check the existence of the top popular movies collection
@task
def update_movie_reviews(db, tmdb_api_key):
    update_reviews(db, tmdb_api_key)


@flow(name="Movie-ETL-History", log_prints=True)
def movie_etl_flow():
    load_dotenv()
    # Database configuration
    mongo_uri = os.getenv('MONGO_URI')
    client = pymongo.MongoClient(mongo_uri)
    db_name = os.getenv('MONGODB_DATABASE', 'default_db_name').replace(' ', '_')
    db = client[db_name]
    
    tmdb_api_key = os.getenv('TMDB_API_KEY')

    release_date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    release_date_to = datetime.now().strftime('%Y-%m-%d')

    # Step 1: Upload movies per week
    extract_and_load_recent_movies(release_date_from, release_date_to)

    # step 2: update new review of top popular movies 
    update_movie_reviews(db, tmdb_api_key, release_date_from, release_date_to)

# Execute the flow
if __name__ == "__main__":
    movie_etl_flow()
