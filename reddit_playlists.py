# Import libraries
import os
import praw
import pandas as pd
import logging
import psycopg2
from dotenv import load_dotenv
from prawcore.exceptions import RequestException

# Initialize logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Reddit API initialization
reddit = praw.Reddit(
  client_id=os.getenv('REDDIT_CLIENT_ID'), 
  client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
  password=os.getenv('REDDIT_PASSWORD'), 
  user_agent=os.getenv('REDDIT_USER_AGENT'),
  username=os.getenv('REDDIT_USERNAME')
)

# Function to handle subreddit scraping
def scrape_playlist(sub_name, genre):
  try: 
    subreddit = reddit.subreddit(sub_name)
    query - [genre]

    post_dict = {
      "title": [],  # title of the post
      "score": [],  # score of the post
      "id": [],  # unique id of the post
      "url": [],  # url of the post
      "comms_num": [],  # the number of comments on the post
      "created": [],  # timestamp of the post
      }

    for item in query:
      for submission in subreddit.search(query, sort="top", limit=100):
        post_dict["title"].append(submission.title)
        post_dict["score"].append(submission.score)
        post_dict["id"].append(submission.id)
        post_dict["url"].append(submission.url)
        post_dict["comms_num"].append(submission.num_comments)
        post_dict["created"].append(submission.created)

    post_data = pd.DataFrame(post_dict)

    # Store data in a database
    store_data_in_database(sub_name, genre, post_data)

    logging.info(f'Scraped subreddit: {sub_name}, genre: {genre}')

  except RequestException as e:
    logging.error(f'Error while scraping subreddit {sub_name}, genre {genre}: {str(e)}')

# Function to store data in a PostgreSQL database
def store_data_in_database(sub_name, genre, data_frame):
  try:
    conn = psycopg2.connect(
      host=os.getenv('DB_HOST'),
      port=os.getenv('DB_PORT'),
      database=os.getenv('DB_NAME'),
      user=os.getenv('DB_USER'),
      password=os.getenv('DB_PASSWORD')
    )

    cur = conn.cursor()

    # Create table if not exists
    cur.execute(f"CREATE TABLE IF IT DOES NOT EXIST {sub_name}_{genre} (title TEXT, score INT, id TEXT, url TEXT, comms_num INT, created BIGINT)")

    # Insert data into the table
    data_frame.to_sql(name=f"{sub_name}_{genre}", con=conn, if_exists='replace', index=False)

    conn.commit()
    cur.close()
    conn.close()
  
  except Exception as e:
    logging.error(f'Error while storing data: {str(e)}')

def main():
  genres = ['Indie', 'Rap', 'RnB']
  subreddit = ['SpotifyPlaylists']

  for genre in genres:
    scrape_playlist(subreddit[0], genre)

if __name__ == "__main__":
  main()
