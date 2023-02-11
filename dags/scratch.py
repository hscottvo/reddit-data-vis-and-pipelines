import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

import praw
from dotenv import dotenv_values

config = dotenv_values("./api_keys/.env")
reddit = praw.Reddit(
    client_id="Yinl04dGAv0dJTSZs0Fvrg",
    client_secret="8Td68YOhU8JYq94Z1Kmmods9pXcFzw",
    user_agent="airflow-learning",
)
wikipage = reddit.subreddit("Music").wiki["musicsubreddits"]
soup = BeautifulSoup(wikipage.content_html, "html.parser")
wiki_list = soup.find("div", {"class": "md wiki"})
lis = wiki_list.find_all("li")
all_a = [i.a for i in lis if i.a is not None]
subreddits = pd.DataFrame([a["href"] for a in all_a if a["href"].startswith("/r/")])
subreddits.columns = ["subreddit"]
subreddits.to_csv("data/subs.csv", index=False)
# hot_posts = reddit.subreddit("MachineLearning").hot(limit=10)
# for post in hot_posts:
#     print(post.title)
# subs = reddit.subreddit("MachineLearning").subscribers
# print(subs)
