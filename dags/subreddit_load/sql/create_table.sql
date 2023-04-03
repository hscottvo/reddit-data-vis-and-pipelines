create table if not exists subreddits (
  subreddit varchar
  , name varchar
  , sub_count int
  , last_updated timestamp
  , primary key (subreddit)
);

