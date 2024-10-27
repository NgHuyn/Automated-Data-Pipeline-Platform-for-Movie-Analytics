from prefect import task, flow
from movie_crawling.fetch_data import fetch_and_save_movie_data
from pipeline.transform import MongoDataExtractor  
from pipeline.load_data import load_data_to_postgres  

@task(retries=2)
def extract_and_load_movies(release_date_from, release_date_to, batch_size=10):
    data = fetch_and_save_movie_data(release_date_from, release_date_to, batch_size)
    return data

@task
def transform_data(movies):
    transformed_data = MongoDataExtractor(movies).transform_data()
    return transformed_data

@task
def load_transformed_data(transformed_data):
    load_data_to_postgres(transformed_data)

@flow(name="Movie-ETL-History", log_prints=True)
def movie_etl_flow():
    release_date_from = '2024-10-20'
    release_date_to = '2024-10-27'
    data = extract_and_load_movies(release_date_from, release_date_to, batch_size=10)
    transformed_data = transform_data(data)
    load_transformed_data(transformed_data)

# Execute the flow
if __name__ == "__main__":
    movie_etl_flow()
