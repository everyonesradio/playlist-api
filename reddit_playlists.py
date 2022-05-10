import os
import praw
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

reddit = praw.Reddit(
  client_id=os.getenv('REDDIT_CLIENT_ID'), 
  client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
  password=os.getenv('REDDIT_PASSWORD'), 
  user_agent=os.getenv('REDDIT_USER_AGENT'),
  username=os.getenv('REDDIT_USERNAME')
)

sub = ['SpotifyPlaylists']

def playlist_genre(genre):
  for s in sub:
    subreddit = reddit.subreddit(s)
    query = [genre]

    for item in query:
      post_dict = {
      "title": [],  # title of the post
      "score": [],  # score of the post
      "id": [],  # unique id of the post
      "url": [],  # url of the post
      "comms_num": [],  # the number of comments on the post
      "created": [],  # timestamp of the post
      }

      for submission in subreddit.search(query, sort="top", limit=100):
        post_dict["title"].append(submission.title)
        post_dict["score"].append(submission.score)
        post_dict["id"].append(submission.id)
        post_dict["url"].append(submission.url)
        post_dict["comms_num"].append(submission.num_comments)
        post_dict["created"].append(submission.created)

        post_data = pd.DataFrame(post_dict)
        post_data.to_csv(s + "_" + item + "_subreddit.csv")

genres = ['Indie', 'Rap', 'RnB']

for i in genres:
  playlist_genre(i)
