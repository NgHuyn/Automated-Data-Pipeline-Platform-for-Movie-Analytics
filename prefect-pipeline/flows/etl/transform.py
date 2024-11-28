import pymongo
import pandas as pd
from dotenv import load_dotenv
import os
import logging
import numpy as np

logging.basicConfig(level=logging.INFO)

class MongoDataExtractor:
    def __init__(self):
        """Initialize and configure MongoDB connection"""
        load_dotenv()
        self.db = self.connect_to_mongo()

    def connect_to_mongo(self):
        """Connect to MongoDB and return the database object"""
        client = pymongo.MongoClient(os.getenv('MONGO_URI'))
        db_name = os.getenv('MONGODB_DATABASE', 'default_db_name').replace(' ', '_')
        return client[db_name]

    def load_collection_as_dataframe(self, collection_name):
        """Load MongoDB collection into a DataFrame"""
        data = list(self.db[collection_name].find({}))
        if not data:
            logging.warning(f"No data found in collection: {collection_name}")
        return pd.DataFrame(data)

    def check_and_mark_processed(self, collection):
        """Check if a collection is processed and mark it if not."""
        if not self.db['processing_flags'].find_one({'collection': collection}):
            self.db['processing_flags'].insert_one({'collection': collection})
            return False
        return True
    
    def process_all_collections(self):
        """Load and transform all specified collections from MongoDB"""
        def transform_movie_reviews(df, movie_details_df):
            """Transform movie reviews"""
            required_columns = ['Movie ID', 'Reviews']
            
            # Check for required columns in the DataFrame
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logging.error(f"Missing columns in DataFrame: {missing_columns}")
                return None

            # Create a mapping from imdb_id to movie_id
            imdb_id_to_movie_id = dict(zip(movie_details_df['imdb_id'], movie_details_df['id']))

            # Check if the top_popular_movies_details collection exists
            if 'top_popular_movies_details' in self.db.list_collection_names():
                top_movie_details_df = self.load_collection_as_dataframe('top_popular_movies_details')[['id', 'imdb_id']]
                top_imdb_id_to_movie_id = dict(zip(top_movie_details_df['imdb_id'], top_movie_details_df['id']))
            else:
                top_imdb_id_to_movie_id = {}

            # Check if the top_popular_movies collection exists
            if 'top_popular_movies' in self.db.list_collection_names():
                top_popular_movies_collection = self.db['top_popular_movies']
            else:
                top_popular_movies_collection = None

            reviews_data = []  # List to store transformed review data

            # Iterate over each row in the df
            for index, row in df.iterrows():
                movie_id = row['Movie ID']
                reviews = row['Reviews']
                mapped_movie_id = imdb_id_to_movie_id.get(movie_id)
                
                if mapped_movie_id is None:
                    mapped_movie_id = top_imdb_id_to_movie_id.get(movie_id)

                    if top_popular_movies_collection is not None:
                        # Delete old imdb_id in top_popular_movies
                        top_popular_movies_collection.delete_one({'imdb_id': mapped_movie_id})

                    if mapped_movie_id is None:
                        logging.warning(f"Movie ID {movie_id} not found in both movie_details and top_popular_movies_details.")
                        continue  # Skip if the Movie ID is not found

                # Iterate over each review and extract relevant information
                for review in reviews:
                    reviews_data.append({
                        'movie_id': mapped_movie_id,
                        'review_summary': review.get('Review Summary'),
                        'review_text': review.get('Review'),
                        'rating': review.get('Rating'),
                        'author': review.get('Author'),
                        'date': review.get('Date') if review.get('Date') != "" else None,  
                        'helpful': review.get('Helpful'),
                        'not_helpful': review.get('Not Helpful')
                    })

            reviews_df = pd.DataFrame(reviews_data)    
            reviews_df['rating'] = reviews_df['rating'].replace('No rating', None)
            reviews_df['rating'] = pd.to_numeric(reviews_df['rating'], errors='coerce')
            reviews_df = reviews_df.replace({np.nan: None})
            reviews_df.drop_duplicates(inplace=True)

            return {'review': reviews_df}

        # Define transformations for each collection
        transformations = {
            'movie_genres': lambda df: {
                'genre': df.drop(columns=['_id']).rename(columns={'id': 'genre_id'}).drop_duplicates()
            } if not self.check_and_mark_processed('movie_genres') else None,
            
            'movie_details': lambda df: {
                'movie': df[['id', 'title', 'budget', 'homepage', 'overview', 'popularity', 'poster_path',
                            'release_date', 'revenue', 'runtime', 'status', 'tagline', 
                            'vote_average', 'vote_count']]
                            .rename(columns={'id': 'movie_id'})
                            .assign(poster_path=lambda x: x['poster_path'].apply(
                                lambda p: f"https://image.tmdb.org/t/p/w500{p}" if p else None))
                            .replace({np.nan: None, '': None})
                            .drop_duplicates(),
                'movie_genre': pd.DataFrame(
                        [(row['id'], g['id']) for _, row in df.iterrows() for g in row['genres']], 
                        columns=['movie_id', 'genre_id']
                    ).drop_duplicates()
            },

            'actor_details': lambda df: {
                'actor': df[['id', 'name', 'gender', 'birthday', 'deathday', 'popularity', 'place_of_birth']]
                            .rename(columns={'id': 'actor_id'})
                            .replace({'gender': {0: 'Not set / not specified', 1: 'Female', 2: 'Male', 3: 'Non-binary'}})  
                            .drop_duplicates()
            },
            
            'director_details': lambda df: {
                'director': df[['id', 'name', 'gender', 'birthday', 'deathday', 'popularity', 'place_of_birth']]
                            .rename(columns={'id': 'director_id'})
                            .replace({'gender': {0: 'Not set / not specified', 1: 'Female', 2: 'Male', 3: 'Non-binary'}})
                            .drop_duplicates()
            },  
            
            'movie_actor_credits': lambda df: {
                'movie_cast': df[['id', 'character', 'order', 'movie_tmdb_id']]
                            .rename(columns={'id': 'actor_id', 'order': 'order_num', 'movie_tmdb_id': 'movie_id'})
                            .replace({np.nan: None, '': None}) 
                            .drop_duplicates()
            },
            
            'movie_director_credits': lambda df: {
                'movie_direction': df[['id', 'known_for_department', 'movie_tmdb_id']]
                            .rename(columns={'id': 'director_id', 'movie_tmdb_id': 'movie_id'})
                            .replace({np.nan: None, '': None}) 
                            .drop_duplicates()
            },
            
            'movie_reviews': lambda df: transform_movie_reviews(df, movie_details_df)
        }

        transformed_data = {}
        # Load movie_details once to use for mapping
        movie_details_df = self.load_collection_as_dataframe('movie_details')[['id', 'imdb_id']]

        # Process each collection and apply transformations
        for collection, transform_func in transformations.items():
            df = self.load_collection_as_dataframe(collection)
            if not df.empty:
                collection_data = transform_func(df)
                if collection_data is not None:
                    transformed_data.update(collection_data)

                if collection not in ['movie_genres', 'processing_flags', 'top_popular_movies', 'top_popular_movies_details']:
                    self.db[collection].delete_many({})

        # Check and mark processed for movie_genres at the end of processing
        self.check_and_mark_processed('movie_genres')

        return transformed_data