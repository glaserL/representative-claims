import argparse
from graphdb import nodes, relations, annotations
from graphdb.graph_util import init_db


def execute(graph, command, args):
    if command == "write_users":
        nodes.write_users(graph, args)
    elif command == "write_tweets":
        nodes.write_tweets(graph, args)
    elif command == "connect_tweets_users":
        relations.connect_tweets_users(graph, args)
    elif command == "write_locations":
        nodes.write_locations(graph, args)
    elif command == "connect_users_cities":
        relations.connect_users_cities(graph, args)
    elif command == "annotate_politicians":
        annotations.annotate_politicians(graph, args)
    elif command == "add_gender_label":
        annotations.add_gender_label(graph, args)
    elif command == "add_relevant_label":
        annotations.add_relevant_label(graph, args)
    elif command == "write_hydrated_tweets":
        nodes.write_hydrated_tweets(graph, args)
    elif command == "connect_tweets_users_by_id":
        relations.connect_tweets_users_by_id(graph, args)
    elif command == "write_nlp":
        annotations.write_nlp(graph, args)
    elif command == "remove_entities":
        annotations.remove_entities(graph, args)
    elif command == "write_sentiment":
        annotations.write_sentiment(graph, args)
    elif command == "write_sentiment_vader":
        annotations.write_sentiment_vader(graph, args)
    elif command == "annotate_gold_frames":
        annotations.annotate_gold_frames(graph, args)
    elif command == "connect_users":
        relations.connect_users(graph, args)
    elif command == "all":
        nodes.write_users(graph, args)
        nodes.write_hydrated_tweets(graph, args)
        relations.connect_tweets_users_by_id(graph, args)
        nodes.write_locations(graph, args)
        relations.connect_users_cities(graph, args)
        annotations.annotate_politicians(graph, args)
        annotations.add_gender_label(graph, args)
        annotations.add_relevant_label(graph, args)
        annotations.remove_entities(graph, args)
        annotations.write_nlp(graph, args)
        annotations.write_sentiment_vader(graph, args)
        annotations.annotate_gold_frames(graph, args)
        relations.connect_users(graph, args)

if(__name__ == "__main__"):

    parser = argparse.ArgumentParser(description='Params')
    parser.add_argument('--commands', nargs = '*', type=str, help= 'interesting attribute to stream line cluttered jsonfiles')
    parser.add_argument('--dg', help='Will delete the whole graph before doing anything.', action='store_true')
    args = parser.parse_args()
    graph = init_db(args.dg)

    if args.commands:
        for command in args.commands:
            execute(graph,command, args)


