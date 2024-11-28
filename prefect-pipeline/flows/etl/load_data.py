import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import logging
from psycopg2.extras import execute_values

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
        logging.error(f"Error connecting to PostgreSQL: {e}", exc_info=True)
        return None

def create_table_if_not_exists(conn, table_name, create_table_query):
    """Create a table if it doesn't exist."""
    with conn.cursor() as cursor:
        try:
            cursor.execute(create_table_query)
            conn.commit()
            logging.info(f"Table {table_name} is ready.")
        except Exception as e:
            logging.error(f"Error creating table {table_name}: {e}", exc_info=True)
            conn.rollback()

def is_table_empty(conn, table_name):
    """Check if a table is empty."""
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1);")
        return not cursor.fetchone()[0]

def filter_existing_ids(conn, table_name, column_name, ids):
    """Filter out existing IDs from the database."""
    query = f"SELECT {column_name} FROM {table_name} WHERE {column_name} = ANY(%s)"
    with conn.cursor() as cursor:
        cursor.execute(query, (ids,))
        existing_ids = set(row[0] for row in cursor.fetchall())
    return [id_ for id_ in ids if id_ not in existing_ids]

def load_data_to_postgres(data: pd.DataFrame, table_name: str):
    """Load data into PostgreSQL table."""
    conn = create_connection()
    if conn is None:
        return

    try:
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
                poster_path TEXT,
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
                FOREIGN KEY (movie_id) REFERENCES movie(movie_id) ON DELETE CASCADE,
                FOREIGN KEY (genre_id) REFERENCES genre(genre_id) ON DELETE CASCADE
            );""",
            'actor': """
            CREATE TABLE IF NOT EXISTS actor (
                actor_id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                gender VARCHAR(50),
                birthday DATE,
                deathday DATE,
                popularity FLOAT,
                place_of_birth TEXT
            );""",
            'director': """
            CREATE TABLE IF NOT EXISTS director (
                director_id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                gender VARCHAR(50),
                birthday DATE,
                deathday DATE,
                popularity FLOAT,
                place_of_birth TEXT
            );""",
            'movie_cast': """
            CREATE TABLE IF NOT EXISTS movie_cast (
                actor_id INTEGER,
                character VARCHAR(100),
                order_num INTEGER,
                movie_id INTEGER,
                FOREIGN KEY (movie_id) REFERENCES movie(movie_id) ON DELETE CASCADE,
                FOREIGN KEY (actor_id) REFERENCES actor(actor_id) ON DELETE CASCADE
            );""",
            'movie_direction': """
            CREATE TABLE IF NOT EXISTS movie_direction (
                director_id INTEGER,
                known_for_department VARCHAR(20),
                movie_id INTEGER,
                FOREIGN KEY (movie_id) REFERENCES movie(movie_id) ON DELETE CASCADE,
                FOREIGN KEY (director_id) REFERENCES director(director_id) ON DELETE CASCADE
            );""",
            'review': """
            CREATE TABLE IF NOT EXISTS review (
                movie_id INTEGER,
                review_summary TEXT,
                review_text TEXT,
                rating FLOAT,
                author VARCHAR(100),
                date DATE,
                helpful FLOAT,
                not_helpful FLOAT,
                FOREIGN KEY (movie_id) REFERENCES movie(movie_id) ON DELETE CASCADE
            );"""
        }

        # Create the tables in order if necessary
        create_tables_in_order(conn, table_queries)

        # Check if the table is empty for 'genre'
        if table_name == 'genre' and not is_table_empty(conn, table_name):
            logging.info(f"Table {table_name} already has data. Skipping data load.")
            return

        # Filter existing IDs for actor or director tables
        if table_name in ['actor', 'director']:
            id_column = 'actor_id' if table_name == 'actor' else 'director_id'
            data_ids = data[id_column].tolist()
            filtered_ids = filter_existing_ids(conn, table_name, id_column, data_ids)
            data = data[data[id_column].isin(filtered_ids)]

        # Insert data into the table
        if not data.empty:
            insert_query = f"INSERT INTO {table_name} ({', '.join(data.columns)}) VALUES %s"
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, data.values.tolist())
            conn.commit()
            logging.info(f"Data loaded successfully into {table_name}.")
        else:
            logging.info(f"No new data to load into {table_name}.")
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()

def create_tables_in_order(conn, table_queries):
    """Create tables in a specified order."""
    table_creation_order = ['genre', 'movie', 'movie_genre', 'actor', 'director', 
                            'movie_cast', 'movie_direction', 'review']
    for table_name in table_creation_order:
        create_table_if_not_exists(conn, table_name, table_queries[table_name])