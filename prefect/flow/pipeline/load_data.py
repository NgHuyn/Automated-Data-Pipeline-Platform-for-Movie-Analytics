import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import logging
from transform import MongoDataExtractor  

logging.basicConfig(level=logging.INFO)

def create_connection():
    """Create a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432')
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return None

def create_table_if_not_exists(conn, table_name, create_table_query):
    """Create a table if it doesn't exist."""
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logging.info(f"Table {table_name} is ready.")
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()

def load_data_to_postgres(data: pd.DataFrame, table_name: str):
    """Load data into PostgreSQL table."""
    conn = create_connection()
    if conn is None:
        return

    # Table creation queries
    table_queries = {
        'genre': """
        CREATE TABLE IF NOT EXISTS genre (
            genre_id INTERGER PRIMARY KEY,
            name VARCHAR(20)
        );""",
        'movie': """
        CREATE TABLE IF NOT EXISTS movie (
            movie_id INTEGER PRIMARY KEY,
            title TEXT,
            budget BIGINT,
            homepage TEXT,
            overview TEXT,
            popularity FLOAT,
            release_date DATE,
            revenue FLOAT,
            runtime INTEGER,
            status VARCHAR(50),
            tagline TEXT,
            vote_average FLOAT,
            vote_count FLOAT
        );""",
        'movie_genre': """
        CREATE TABLE IF NOT EXISTS movie_genre (
            movie_id INTEGER,
            genre_id INTEGER,
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
        );""",
        'movie_cast': """
        CREATE TABLE IF NOT EXISTS movie_cast (
            actor_id INTEGER,
            character VARCHAR(50),
            order_num INTEGER,
            movie_id INTEGER,
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
        );""",
        'movie_direction': """
        CREATE TABLE IF NOT EXISTS movie_direction (
            director_id INTEGER,
            known_for_department VARCHAR(20),
            movie_id INTEGER,
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
        );""",
        'actor': """
        CREATE TABLE IF NOT EXISTS actor (
            actor_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            gender VARCHAR(50),
            birthday DATE,
            deathday DATE,
            popularity FLOAT,
            place_of_birth TEXT
        );""",
        'director': """
        CREATE TABLE IF NOT EXISTS director (
            director_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            gender VARCHAR(50),
            birthday DATE,
            deathday DATE,
            popularity FLOAT,
            place_of_birth TEXT
        );""",
        'review': """
        CREATE TABLE IF NOT EXISTS review (
            movie_id INTEGER,
            review_summary TEXT,
            review_text TEXT,
            rating SMALLINT,
            author VARCHAR(50),
            date DATE,
            helpful INTEGER,
            not_helpful INTEGER,
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
        );"""
    }

    # Create the corresponding table if it does not exist
    if table_name in table_queries:
        create_table_if_not_exists(conn, table_name, table_queries[table_name])

    cursor = conn.cursor()
    
    # Insert statement
    insert_query = f"""
    INSERT INTO {table_name} ({', '.join(data.columns)}) 
    VALUES ({', '.join(['%s'] * len(data.columns))})
    """

    try:
        # Using executemany for better performance
        cursor.executemany(insert_query, data.values.tolist())  
        
        conn.commit()  # Commit the changes
        logging.info(f"Data loaded successfully into {table_name}.")

    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        conn.rollback()  # Rollback in case of error

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_dotenv()  # Load environment variables

    extractor = MongoDataExtractor()
    transformed_data = extractor.process_all_collections()

    # Load each table into PostgreSQL
    if 'genre' in transformed_data:
        load_data_to_postgres(transformed_data['genre'], 'genre')
    if 'movie' in transformed_data:
        load_data_to_postgres(transformed_data['movie'], 'movie')
    if 'movie_genre' in transformed_data:
        load_data_to_postgres(transformed_data['movie_genre'], 'movie_genre')
    if 'movie_cast' in transformed_data:
        load_data_to_postgres(transformed_data['movie_cast'], 'movie_cast')
    if 'movie_direction' in transformed_data:
        load_data_to_postgres(transformed_data['movie_direction'], 'movie_direction')
    if 'actor' in transformed_data:
        load_data_to_postgres(transformed_data['actor'], 'actor')
    if 'director' in transformed_data:
        load_data_to_postgres(transformed_data['director'], 'director')
    if 'review' in transformed_data:
        load_data_to_postgres(transformed_data['review'], 'review')
