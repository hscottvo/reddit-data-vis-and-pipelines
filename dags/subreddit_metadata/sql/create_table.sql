create table if not exists subscriber_count (
  sub_count_id serial
  , subreddit varchar
  , sub_count int
  , date date
  , primary key (sub_count_id)
);

