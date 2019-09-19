from py2neo.data import Relationship
from tqdm import tqdm

from graphdb import graph_util


def connect_tweets_users(graph, args):
    relationships = []
    user_nodes = graph_util.query_label_to_dict(graph, "User", "screen_name")
    tweet_nodes = graph.nodes.match("Tweet")


    print("Creating relationships between users via tweets..")


    for tweet in tqdm(tweet_nodes):
        if tweet['screen_name'] in user_nodes.keys():
            user = user_nodes[tweet['screen_name']] # relate the tweet
            relationships.append(Relationship(user, "TWEETED", tweet))
        if isinstance(tweet['mentions'], list):
            for mention in tweet['mentions']:
                if mention in user_nodes.keys():
                    mentioned_user = user_nodes[mention] # grabbing the user object by screen_name
                    relationships.append(Relationship(tweet, "MENTIONS", mentioned_user))
    print("Done.")

    #### lets write mentions
    print("Writing %d relationships to database.." % len(relationships))
    graph_util.create_in_batch(graph, relationships)
    print("Done.")


def connect_tweets_users_by_id(graph, args):
    relationships = []
    user_nodes = graph_util.query_label_to_dict(graph, "User", "user_id")
    tweet_nodes = graph.nodes.match("Tweet")


    print("Creating relationships between users via tweets..")


    for tweet in tqdm(tweet_nodes):
        if tweet['user_id'] in user_nodes.keys():
            user = user_nodes[tweet['user_id']] # relate the tweet
            relationships.append(Relationship(user, "TWEETED", tweet))
        if isinstance(tweet['mentions'], list):
            for mention in tweet['mentions']:
                if mention in user_nodes.keys():
                    mentioned_user = user_nodes[mention] # grabbing the user object by screen_name
                    relationships.append(Relationship(tweet, "MENTIONS", mentioned_user))
    print("Done.")

    #### lets write mentions
    print("Writing %d relationships to database.." % len(relationships))
    graph_util.create_in_batch(graph, relationships)
    print("Done.")


def connect_users_cities(graph, args):
    city_dict = graph_util.query_label_to_dict(graph, "City", "name")
    users = graph.nodes.match("User")
    relationships = []
    print("Creating IS_FROM relationships..")
    for user in tqdm(users):
        for city in user['city']:
            relationships.append(Relationship(user, "IS_FROM", city_dict[city]))
    print("Done.")
    print("Writing %d relationships.. " % len(relationships))
    graph_util.create_in_batch(graph, relationships)
    print("Done.")


def connect_users(graph, args):
    print("Connecting user nodes directly..")
    graph.run("""
    MATCH (p1:User)-[r1:TWEETED]->()-[r2:MENTIONS]->(p2:User)
    MERGE (p1)-[r:MENTIONED]->(p2)
    ON CREATE SET r.weight = 1
    ON MATCH SET r.weight = r.weight + 1""")
    print("Done.")