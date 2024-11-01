import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import logging

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

def is_table_empty(conn, table_name):
    """Check if a table is empty."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1);")
    result = cursor.fetchone()[0]
    cursor.close()
    return not result  # Return True if table is empty

def load_data_to_postgres(data: pd.DataFrame, table_name: str):
    """Load data into PostgreSQL table."""
    conn = create_connection()
    if conn is None:
        return

    # Table creation queries
    table_queries = {
        'genre': """
        CREATE TABLE IF NOT EXISTS genre (
            genre_id INTEGER PRIMARY KEY,
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
            rating FLOAT,
            author VARCHAR(50),
            date DATE,
            helpful FLOAT,
            not_helpful FLOAT,
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
        );"""
    }

    # Create the corresponding table if it does not exist
    if table_name in table_queries:
        create_table_if_not_exists(conn, table_name, table_queries[table_name])

    # Check if table is empty for 'genre' table
    if table_name == 'genre' and not is_table_empty(conn, table_name):
        logging.info(f"Table {table_name} already has data. Skipping data load.")
        conn.close()
        return  # Exit function early if data exists

    cursor = conn.cursor()
    
    # check if actor and director already exists
    if table_name == 'actor':
        existing_actors_query = "SELECT actor_id FROM actor WHERE actor_id = ANY(%s)"
        existing_ids = data['actor_id'].tolist()
    elif table_name == 'director':
        existing_directors_query = "SELECT director_id FROM director WHERE director_id = ANY(%s)"
        existing_ids = data['director_id'].tolist()
    else:
        existing_ids = []

    if existing_ids:
        cursor.execute(existing_actors_query if table_name == 'actor' else existing_directors_query, (existing_ids,))
        existing_ids_in_db = set(row[0] for row in cursor.fetchall())
        data = data[~data['actor_id'].isin(existing_ids_in_db)] if table_name == 'actor' else data[~data['director_id'].isin(existing_ids_in_db)]

    # Insert statement
    insert_query = f"""
    INSERT INTO {table_name} ({', '.join(data.columns)}) 
    VALUES ({', '.join(['%s'] * len(data.columns))})
    """

    try:
        if not data.empty:
            # Using executemany for better performance
            cursor.executemany(insert_query, data.values.tolist())  
            conn.commit()  # Commit the changes
            logging.info(f"Data loaded successfully into {table_name}.")
        else:
            logging.info(f"No new data to load into {table_name}.")

    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        conn.rollback()  # Rollback in case of error

    finally:
        cursor.close()
        conn.close()