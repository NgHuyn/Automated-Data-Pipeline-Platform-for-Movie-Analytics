from .base_scraper import BaseScraper
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
import math
from bs4 import BeautifulSoup
from datetime import datetime
from .utils import setup_reviews_logger

# ReviewsScraper class to fetch reviews for each movie
class MovieReviewScraper(BaseScraper):
    def __init__(self, movie_id, total_reviews=0, last_date_review=None):
        super().__init__()  # Call the base class constructor
        self.movie_id = movie_id
        self.clicks = 0  # Initialize click counter
        self.movie_info = { 
            'Movie ID': movie_id,
            'Reviews': []
        }
        self.total_reviews = total_reviews
        self.last_date_review = last_date_review

        self.is_scraping = True  # Flag to manage scraping status

        self.logger = setup_reviews_logger(movie_id) 
        self.logger.info("Fetching reviews for movie_id: %s", movie_id)

    def fetch_reviews(self):
            total_reviews = 0
            try:
                review_url = f"https://www.imdb.com/title/{self.movie_id}/reviews/?sort=submission_date%2Cdesc&dir=desc"
                self.driver.get(review_url)

                self.logger.info("Accessed URL: %s", review_url)

                # Check if there are any reviews available
                total_reviews = self._get_total_reviews()
                if total_reviews == 0:
                    self.logger.info("No reviews found for Movie ID %s", self.movie_id)
                    return None
                try:
                    if total_reviews <= self.total_reviews:
                        self.logger.info("No new reviews found for Movie ID %s", self.movie_id)
                        return None
                except Exception as e:
                    self.logger.error("Error get total")
                # new_reviews_count = total_reviews - self.movie_info['Total Reviews']
                new_reviews_count = total_reviews - self.total_reviews
                self.total_reviews = total_reviews
                self.logger.info("%d new reviews found for Movie ID %s", new_reviews_count, self.movie_id)

                # If reviews are available, attempt to load all or more reviews
                self._load_reviews(new_reviews_count)  # Load more reviews by clicking the button

                wait_time = self._calculate_wait_time(10, self.clicks)  # Adjust wait time based on click count
                time.sleep(wait_time)

                # Extract reviews from the loaded page
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')

                # Extract reviews for the current movie and accumulate total_reviews
                self.movie_info, num_reviews = self._extract_reviews(soup, self.movie_id, self.last_date_review, new_reviews_count)

                if num_reviews == 0:
                    self.logger.warning(f"No reviews found for Movie ID: {self.movie_id}.")
                    return None

                if total_reviews - num_reviews != 0:
                    self.logger.warning('Missing %d reviews', total_reviews - num_reviews)
                self.logger.info('Movie %s has %d/%d reviews', self.movie_id, num_reviews, total_reviews)

                # self.movie_info['Last Date Review'] = self.movie_info['Reviews'][0]['Date'] #updating last date review
                for i in range(len(self.movie_info['Reviews'])):
                    if 'Date' in self.movie_info['Reviews'][i] and self.movie_info['Reviews'][i]['Date']:
                        # self.movie_info['Last Date Review'] = self.movie_info['Reviews'][i]['Date'] #updating last date review
                        self.last_date_review = self.movie_info['Reviews'][i]['Date'] #updating last date review

                        break  
                    else:
                        self.logger.warning(f"Review {i} is missing a date.")
                # self.movie_info['Total Reviews'] = num_reviews #updating new total reviews
                # self.total_reviews = num_reviews #updating new total reviews


            except Exception as e:
                self.logger.error("Error in fetch_reviews: %s", str(e))
            finally:
                self.close_driver()
                self.is_scraping = False
            return self.movie_info

    def _get_total_reviews(self):
        """Fetch the total number of reviews from the page."""
        try:
            # Attempt to find the total reviews element by data-testid
            total_reviews_element = WebDriverWait(self.driver, 2).until(
                EC.presence_of_element_located((By.XPATH, '//div[@data-testid="tturv-total-reviews"]'))
            )
            total_reviews_text = total_reviews_element.text.split()[0]  # Get the number part
            self.logger.info("Total reviews found: %s", total_reviews_text)
            return int(total_reviews_text.replace(',', ''))
        except Exception as e:
            # If the first attempt fails, check the alternative method
            try:
                total_reviews_element = WebDriverWait(self.driver, 2).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@class="header"]//span'))
                )
                total_reviews_text = total_reviews_element.text.split()[0]  # Get the number part
                self.logger.info("Total reviews found: %s", total_reviews_text)
                return int(total_reviews_text.replace(',', ''))
            except Exception as e:
                self.logger.error("Error fetching total reviews: %s", e)
                return 0  # Default to 0 if neither element is found

    def _load_reviews(self, new_reviews_count):
        def click_button(xpath, name, wait=5):
            """Helper function to find and click a button if available."""
            try:
                button = WebDriverWait(self.driver, wait).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'}); arguments[0].click();", 
                    button
                )
                time.sleep(4)  # Wait for content to load after clicking
                self.logger.info(f"Clicked '{name}' button successfully.")
                return True
            except Exception:
                self.logger.info(f"No '{name}' button found.")
                return False
        if new_reviews_count <= 0:
            self.logger.info("No new reviews found for Movie ID %s", self.movie_id)
            return
        
        if new_reviews_count > 25:
            # 1. Try clicking the 'All' button first
            if click_button('//*[@id="__next"]/main/div/section/div/section/div/div[1]/section[1]/div[3]/div/span[2]/button/span/span', 'All'):
                self._scroll_to_load_all()  # Scroll if 'All' button is clicked
                return  # Stop after 'All' is clicked

            # 2. Try clicking the 'More' button if it exists
            if click_button('//*[@id="__next"]/main/div/section/div/section/div/div[1]/section[1]/div[3]/div/span[1]/button/span/span', 'More'):
                return # Stop after 'More' is clicked

            # 3. Continuously click 'Load More' button until it no longer appears
            clicks_needed = math.ceil(new_reviews_count / 25)
            for _ in range(clicks_needed):
                if not click_button('//*[@id="load-more-trigger"]', 'Load More'):
                    self.logger.info("No more 'Load More' buttons found.")
                    break # Exit the loop when 'Load More' is no longer available
            return
        else: 
            return


    def _scroll_to_load_all(self):
        """Scroll the page slowly to ensure all reviews are loaded."""
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        self.logger.info("Starting to scroll to load all reviews.")

        while True:
            # Scroll to the bottom of the page
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)  # Wait for new content to load after each scroll

            # Check if the new page height is the same as the previous one
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:  # Stop if no more content is being loaded
                self.logger.info("Reached the bottom of the page, all reviews loaded.")
                break
            last_height = new_height

    def _calculate_wait_time(self, base_wait_time, clicks):
        """
        Calculate an adaptive wait time based on the number of clicks.
        As the number of clicks increases, the wait time grows exponentially to accommodate website lag.
        """
        growth_factor = 1.2  # Exponential growth factor
        additional_wait_time = base_wait_time * (growth_factor ** (clicks // 10))  # Increase wait time every 10 clicks
        
        return base_wait_time + additional_wait_time

    def _extract_reviews(self, soup, movie_id, last_date, new_reviews_count):
        # Attempt to extract reviews using the primary selector
        reviews = soup.select('article.user-review-item')

        # If no reviews found, try to load more reviews
        if not reviews:  # Attempt to load more reviews
            reviews = soup.select('div.lister-item.mode-detail.imdb-user-review')
            if not reviews:  # If still no reviews available
                self.logger.warning(f"No reviews found for {movie_id}.")
                return 0  # Return 0 if no reviews found

        # Parse reviews and add them to the movie_info['Reviews'] list
        count = 0
        for review in reviews:
            # Determine the button type (all vs. load more) based on the presence of specific elements
            button_type = "load_more" if review.select_one('span.rating-other-user-rating span') else "all"
            parsed_review = self._parse_review(review, button_type)
            
            try:
                if last_date is not None:
                    # last_date = datetime.strptime(last_date, "%Y-%m-%d")
                    # if parsed_review['Date'] <= last_date:
                    #     break
                    if count >= new_reviews_count:
                        break
            except Exception:
                self.logger.error(f"Error at comparison Date")
            # Append the parsed review to the 'Reviews' list
            self.movie_info['Reviews'].append(parsed_review)
            count += 1

        self.logger.info(f"Processed {count} reviews for Movie ID: {movie_id}.")
        return self.movie_info, count # Return the number of reviews processed
    
    def convert_to_int(self, human_readable):
        """Convert human-readable numbers to integers."""
        if 'K' in human_readable:
            return int(float(human_readable.replace('K', '').strip()) * 1000)
        elif 'M' in human_readable:
            return int(float(human_readable.replace('M', '').strip()) * 1000000)
        else:
            return int(human_readable.strip())

    def convert_date_format(self, date_str, button_type):
        desired_format = "%Y-%m-%d"

        if button_type == "load_more":
            current_format = "%d %B %Y"
        elif button_type == "all":
            current_format = "%b %d, %Y"

        date_object = datetime.strptime(date_str, current_format)
        return date_object.strftime(desired_format)

    def _parse_review(self, review, button_type):
        """
        Extract information from the review based on its type (load_more or all)
        """
        # Initialize helpful votes to 0
        found_helpful = 0
        not_helpful = 0

        # Parse review based on the button type
        if button_type == "load_more":
            review_rating = review.select_one('span.rating-other-user-rating span').get_text(strip=True) if review.select_one('span.rating-other-user-rating span') else 'No rating'
            # review_summary = review.select_one('a.title').get_text(strip=True) if review.select_one('a.title') else 'No summary'
            review_summary = review.select_one('a.title').get_text(strip=True) if review.select_one('a.title') else 'No summary'
            review_text = review.select_one('div.text.show-more__control').get_text(strip=True) if review.select_one('div.text.show-more__control') else 'No content'
            author_tag = review.select_one('span.display-name-link a').get_text(strip=True) if review.select_one('span.display-name-link a') else 'Unknown Author'
            review_date = self.convert_date_format(review.select_one('span.review-date').get_text(strip=True), button_type) if review.select_one('span.review-date') else 'No date'
        else:
            review_rating = review.select_one('span.ipc-rating-star--rating').get_text(strip=True) if review.select_one('span.ipc-rating-star--rating') else 'No rating'
            review_summary = review.select_one('h3.ipc-title__text')
            review_summary = review_summary.get_text(strip=True) if review_summary else 'No summary'
            review_text = review.select_one('div.ipc-html-content-inner-div').get_text(strip=True) if review.select_one('div.ipc-html-content-inner-div') else 'No content'
            author_tag = review.select_one('a[data-testid="author-link"]').get_text(strip=True) if review.select_one('a[data-testid="author-link"]') else 'Unknown Author'
            review_date = self.convert_date_format(review.select_one('li.review-date').get_text(strip=True), button_type) if review.select_one('li.review-date') else 'No date'

        # Extract helpful votes
        if button_type == "load_more":
            helpful_text = review.select_one('div.actions.text-muted').get_text(strip=True) if review.select_one('div.actions.text-muted') else ''
            match = re.search(r'(\d+) out of (\d+) found this helpful', helpful_text)
            if match:
                found_helpful = int(match.group(1))
                not_helpful = int(match.group(2)) - found_helpful
        else:
            found_helpful = self.convert_to_int(review.select_one('span.ipc-voting__label__count--up').get_text(strip=True)) if review.select_one('span.ipc-voting__label__count--up') else 0
            not_helpful = self.convert_to_int(review.select_one('span.ipc-voting__label__count--down').get_text(strip=True)) if review.select_one('span.ipc-voting__label__count--down') else 0

        # Return the review information in the expected format
        return {
            'Review Summary': review_summary,
            'Review': review_text,
            'Rating': review_rating,
            'Author': author_tag,
            'Date': review_date,
            'Helpful': found_helpful,
            'Not Helpful': not_helpful
        }