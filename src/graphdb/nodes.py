import json
from tqdm import tqdm
from datetime import datetime
from py2neo.data import Node, Relationship

import re
import time
import pickle
#################################################
### DATA MODEL
#################################################
from graphdb import graph_util


def create_user_node(user_json):
    return Node("User",
    user_id= user_json["id"],
    name= user_json["name"],
    screen_name= user_json["screen_name"],
    location= user_json["location"],
    description= user_json["description"],
    followers_count= user_json["followers_count"],
    friends_count= user_json["friends_count"],
    created_at= user_json["created_at"],
    verified= user_json["verified"],
    statuses_count= user_json["statuses_count"],
    lang= user_json["lang"],
    profile_image_url_https= user_json["profile_image_url_https"],
    state= user_json["state"],
    city= user_json["city"],
    gender= user_json["gender"].get("name", None),
    gender_probability= user_json["gender"].get("probability", None))



def create_tweet_node(tweet):
    return Node("Tweet",
    screen_name = tweet['username'],
    date = str(tweet['date']),
    retweets = tweet['retweets'],
    favorites = tweet['favorites'],
    text = tweet['text'],
    geo = tweet['geo'],
    mentions = tweet['mentions'],
    hashtags = tweet['hashtags'],
    tweet_id = tweet['id'],
    permalink = tweet['permalink'],
    quartal = tweet['quartal'],
    week = tweet['week'],
    links = tweet['links']
    # , POSTagged = tweet['POSTagged']
    )
def create_hydrated_tweet(tweet):
    return Node('Tweet',
        screen_name = tweet['user']['screen_name'],
        user_id = tweet['user']['id'],
        date = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')),
        tweet_id = tweet['id'],
        text = tweet['text'],
        hashtags = [hashtag['text'] for hashtag in tweet['entities']['hashtags']],
        mentions = [user['id'] for user in tweet['entities']['user_mentions']],
        links = [link['expanded_url'] for link in tweet['entities']['urls']],
        media = [media['url'] for media in tweet['entities']["media"]] if ("media" in tweet['entities'].keys()) else [],
        source = tweet['source'],
        # geo = tweet['geo'],
        # coordinates = tweet['coordinates'],
        # place = tweet['place'],
        retweets = tweet['retweet_count'],
        favorites = tweet['favorite_count'],
        lang = tweet['lang'])


#################################################
### WRITE FUNCTIONS
#################################################


def write_users(graph, args):
    print("Reading Userobjects..")
    users = None
    with open("../data/users_with-loc_with-gender.json", encoding='utf-8', mode = 'r') as f:
        inf = f.read()
        users = json.loads(inf)
    print("Read.")

    user_nodes = []
    print("Creating User nodes..")
    for user in tqdm(users):
        user_nodes.append(create_user_node(user))
    print("Done.")
    print("Writing %d users to database.." % len(user_nodes))
    graph_util.create_in_batch(graph, user_nodes)

    print("Done.")


def write_tweets(graph, args):
    print("WARNING, DEPRECTED")

    print("Reading tweets..")
    tweets = None
    with open("../../Datar/twitter/180524_data_posstagged.pickle", mode='rb') as f:
        data = f.read()
        tweets = pickle.loads(data)


    tweets.fillna("N/A",inplace=True) # stop neo4j from breaking!!
    print("Done.")
    print("Creating Tweet nodes..")
    tweet_nodes = []
    with tqdm(total=len(tweets)) as pbar:
        for index, tweet in tweets.iterrows():
            tweet_nodes.append(create_tweet_node(tweet))
            pbar.update(1)
    print("Done.")
    print("Writing %d tweets to database.." % len(tweet_nodes))
    graph_util.create_in_batch(graph, tweet_nodes)
    print("Done.")

def write_hydrated_tweets(graph, args):

    print("Reading tweets..")
    tweets = None
    with open("../data/tweets_fully_hydrated.json", encoding='utf-8',mode='r') as f:
        tweets = json.load(f)
    print("Done.")
    print("Creating Tweet nodes..")
    tweet_nodes = []
    with tqdm(total=len(tweets)) as pbar:
        for tweet in tweets:
            tweet_nodes.append(create_hydrated_tweet(tweet))
            pbar.update(1)
    print("Done.")
    print("Writing %d tweets to database.." % len(tweet_nodes))
    graph_util.create_in_batch(graph, tweet_nodes)
    print("Done.")



def write_locations(graph, args):
#### and let's write the location community.

    def create_state_node_dict(states):
        result = {}
        for state in set(states):
            result[state] = Node("State", name = state)
        return result

    def load_city_mapping():
        """ closed in function to get data
        """
        city_mapping = {}

        find_state = re.compile("(\(\w{2}\))")
        find_city = re.compile("(.+ )")
        with open("../data/german_cities_raw.txt", encoding = 'utf-8', mode='r') as f:
            for entry in f:
                city = find_city.findall(entry)[0].strip()
                state = find_state.findall(entry)[0].replace("(","").replace(")","")
                city_mapping[city] = state
        return city_mapping


    city_mapping = load_city_mapping()
    cities = []
    states = []
    for user in graph.nodes.match("User"):
        cities.extend(user['city'])
        states.extend(user['state'])


    states = create_state_node_dict(states)
    print("Writing state nodes..")
    graph_util.create_in_batch(graph, list(states.values()))
    print("Done.")
    city_nodes = []
    for city in set(cities):
        city_nodes.append(Node("City", name=city))
    print("Creating city nodes..")
    graph_util.create_in_batch(graph, city_nodes)
    print("Done.")
    city_nodes
    city_to_states = []
    for city_node in city_nodes:
        city_to_states.append(Relationship(city_node, "IS_IN", states[city_mapping[city_node['name']]]))
    print("Creating city to state relationships..")
    graph_util.create_in_batch(graph, city_to_states)
    print("Done.")
    #
