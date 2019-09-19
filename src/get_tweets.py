"""
Because we're not allowed to share raw tweets, we provide a small script that gets those tweets based on a
list of ids. Don't forget to configure the config json file with your developer account.
"""

import tweepy
import os
import json

config = json.load(open("config.json", encoding="utf-8"))

consumer_key = config["twitter"]["consumer_key"]
consumer_secret = config["twitter"]["consumer_secret"]

access_token_key = config["twitter"]["access_token_key"]
access_token_secret = config["twitter"]["access_token_secret"]

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token_key, access_token_secret)

api = tweepy.API(auth_handler=auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
print("Established connection to twitter.")

print("Done.")
tweet_ids = []
with open("../data/tweet_ids.txt", mode='r') as f:
    for line in f:
        tweet_ids.append(line.strip())

print(f"Read {len(tweet_ids)} tweet ids from file.")

id_batches = [tweet_ids[i:i + 100] for i in range(0, len(tweet_ids), 100)]

print(f"Starting to get {len(tweet_ids)} tweets.")


def append_to_json(json_file_path, data_to_extend):
    if os.path.exists(json_file_path):
        with open(json_file_path, "r", encoding="utf-8") as inf:
            temp_json = json.load(inf)
    else:
        temp_json = []
    temp_json.extend(data_to_extend)
    with open(json_file_path, "w", encoding="utf-8") as outf:
        json.dump(temp_json, outf, indent=2, ensure_ascii=False)


max_buffer_size = 1000
buffer = []
already_scraped = 0
for i, batch in enumerate(id_batches):
    response = api.statuses_lookup(batch)
    if any(c is None for c in response):
        print(f"WARNING, missing values in batch #{i}")
    for tweet in response:
        content = tweet._json
        buffer.append(content)
        already_scraped += 1
    if len(buffer) > max_buffer_size:
        print(f"Adding {len(buffer)} tweets, already scraped approx. {already_scraped}, {len(tweet_ids)-already_scraped} remaining.")
        append_to_json("../data/tweets_fully_hydrated.json", buffer)
        buffer.clear()
