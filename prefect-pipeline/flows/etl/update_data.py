from movie_crawling.crawl_reviews import MovieReviewScraper
from movie_crawling.crawl_movies import MoviesScraper
from movie_crawling.tmdb_api import TMDBApi
from datetime import datetime, timedelta
import logging

def check_top_popular_movies(db):
    collection_name = 'top_popular_movies'
    if collection_name not in db.list_collection_names():
        return None  # Collection does not exist
    return list(db[collection_name].find().limit(10))

def get_top_10_movies(release_date_from, release_date_to):
    # current popular movies
    popular_movies = MoviesScraper(release_date_from, release_date_to).fetch_movies(limit=30)
    return popular_movies

def update_db(db, imdb_id, type_update, new_reviews, total_reviews=0, last_date_review=None):
    if type_update == 'update_db_reviews':
        db['movie_reviews'].update_one(
            {'Movie ID': imdb_id},
            {
                '$addToSet': {'Reviews': {'$each': new_reviews['Reviews']}}

            },
            upsert=True
        )
    elif type_update == 'insert_db_reviews':
        try:
            if isinstance(new_reviews, list):
                db['movie_reviews'].insert_many(new_reviews, ordered=False)
            else:
                db['movie_reviews'].insert_one(new_reviews)
            logging.info(f"Inserted data into movie_reviews.")
        except Exception as e:
            logging.error(f"Error saving to movie_reviews: {e}")
    elif type_update == 'update_db_top_popular': 
        db['top_popular_movies'].update_one(
            {'imdb_id': imdb_id},
            {
                '$set': {
                    'total_reviews': total_reviews,
                    'last_date_review': last_date_review
                }
            },
            upsert=True
        )
    elif type_update == 'insert_db_top_popular':
        db['top_popular_movies'].insert_one({
                    'imdb_id': imdb_id,
                    'total_reviews': total_reviews,
                    'last_date_review': last_date_review,
        })
        logging.info(f"Inserted new movie info for ID: {imdb_id}.")

def update_reviews(db, tmdb_api_key, release_date_from, release_date_to):
    tmdb_api = TMDBApi(api_key=tmdb_api_key)

    # get new top 10
    popular_movies = get_top_10_movies(release_date_from, release_date_to)
    # check if db top popular exists
    existing_movies = check_top_popular_movies(db)
    
    # Case 1: if the collection exists
    if existing_movies:
        logging.info("Updating reviews for existing popular movies.")

        # update reviews for older top 10 popular before updating new top 10
        for movie in existing_movies:
            imdb_id = movie['imdb_id']
            pre_total_reviews=movie.get('total_reviews')
            # Fetch `last_date_review` and `total_reviews` from database
            try:
                fetch_reviews = MovieReviewScraper(movie_id=imdb_id, total_reviews=movie.get('total_reviews'), last_date_review=movie.get('last_date_review'))
                new_reviews = fetch_reviews.fetch_reviews()

                if new_reviews is not None and len(new_reviews['Reviews']) > 0:
                    if pre_total_reviews == 0: # if there isn't having any reviews
                        update_db(db, imdb_id, 'insert_db_reviews', new_reviews)
                    else: # Update the reviews for the db movie_reviews if the previous had reviews
                        update_db(db, imdb_id, 'update_db_reviews', new_reviews)

                    logging.info(f"Updated top_popular_movies for {imdb_id}.")
                else:
                    logging.info(f"No new reviews for {imdb_id}.")
            except Exception as e:
                logging.error(f"Error fetching reviews for movie ID {imdb_id}: {e}")
        count_movie = 0

        # Fetch reviews for new top 10 movies
        for movie in popular_movies:
            imdb_id = movie['Movie ID']

            # Check if imdb_id exists in the database
            tmdb_id = tmdb_api.find_tmdb_id_by_imdb_id(imdb_id)
            if not tmdb_id:
                logging.warning(f"TMDB ID not found for IMDB ID {imdb_id}. Skipping.")
                continue

            # If we have enough movies, stop
            if count_movie >= 10:
                break

            logging.info(f"Fetching new reviews for movie ID: {imdb_id}")
            
            try:
                fetch_reviews = MovieReviewScraper(movie_id=imdb_id, total_reviews=0, last_date_review=None)
                new_reviews = fetch_reviews.fetch_reviews()

                if new_reviews and len(new_reviews['Reviews']) > 0:
                    # Update the reviews for the db movie_reviews
                    update_db(db, imdb_id, 'update_db_reviews', new_reviews)
                    
                    # Update db top_popular_movies
                    update_db(db, imdb_id, 'update_db_top_popular', new_reviews, fetch_reviews.total_reviews, fetch_reviews.last_date_review)
                    logging.info(f"Added {len(new_reviews['Reviews'])} new reviews for {imdb_id}.")
                    logging.info(f"Updated top_popular_movies for {imdb_id}.")

                    # Update db top_popular_movies_details
                    db['top_popular_movies_details'].insert_one({
                                'imdb_id': imdb_id,
                                'id': tmdb_id
                    })

                else:
                    # Update db top_popular_movies
                    if fetch_reviews.total_reviews != 0:
                        update_db(db, imdb_id, 'update_db_top_popular', new_reviews, fetch_reviews.total_reviews, fetch_reviews.last_date_review)
                    else:
                        update_db(db, imdb_id, 'update_db_top_popular', new_reviews)

                    logging.info(f"No new reviews for {imdb_id}.")

                count_movie += 1
            except Exception as e:
                logging.error(f"Error fetching reviews for movie ID {imdb_id}: {e}")
                
        existing_imdb_ids = {movie['imdb_id'] for movie in existing_movies}

        # Delete movies are outdated
        for existing_id in existing_imdb_ids:
            db['top_popular_movies'].delete_one({'imdb_id': existing_id})
            print(f"Removed movie with imdb_id: {existing_id} from top_popular_movies.")

    # Case 2: Collection does not exist
    else:
        count_movie = 0
        logging.info("No existing popular movies found. Fetching new top 10 popular movies.")
        for movie in popular_movies:
            imdb_id = movie['Movie ID']
            
            # Check if imdb_id exists in the database
            tmdb_id = tmdb_api.find_tmdb_id_by_imdb_id(imdb_id)
            if not tmdb_id:
                logging.warning(f"TMDB ID not found for IMDB ID {imdb_id}. Skipping.")
                continue

            if count_movie >= 10:
                break

            logging.info(f"Fetching reviews for new movie ID: {imdb_id}")
            try:
                fetch_reviews = MovieReviewScraper(movie_id=imdb_id)
                new_reviews = fetch_reviews.fetch_reviews()

                # Insert to db top_popular_movies
                update_db(db, imdb_id, 'insert_db_top_popular', new_reviews, fetch_reviews.total_reviews, fetch_reviews.last_date_review)

                # Update db top_popular_movies_details
                db['top_popular_movies_details'].insert_one({
                            'imdb_id': imdb_id,
                            'id': tmdb_id
                })
                count_movie += 1
            except Exception as e:
                logging.error(f"Error fetching reviews for movie ID {imdb_id}: {e}")
