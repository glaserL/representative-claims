import json
from py2neo.data import Node, Relationship
from tqdm import tqdm
from datetime import datetime

from graphdb import graph_util
from graphdb.vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


def annotate_politicians(graph, args):
    with open("../data/politician_user_handles.json", encoding = 'utf-8', mode='r') as f:
        inf = f.read()
        temp = json.loads(inf)
    parties = {}
    politicians = {}
    ## flip the dict around to have things more readable
    for party in temp.keys():
        parties[party] = Node('Party', name=party) ## create party nodes along the way
        for politician in temp[party]:
            politicians[politician] = party

    relationships = []
    graph_util.create_in_batch(graph, list(parties.values()))
    print("Done.")

    print("Creating party membership relations..")

    for politician in tqdm(politicians.keys()):

        politician_node = graph.nodes.match("User").where("_.screen_name = \"%s\"" % politician).first()

        if politician_node != None:
            ### this needs to be done when updating anything, this is bullshit.
            graph.merge(politician_node)
            politician_node.add_label("Politician")
            graph.push(politician_node)

            relationships.append(Relationship(politician_node, "IS_MEMBER_OF", parties[politicians[politician]]))
    print("Done.")
    print("Writing %d party membership relations to database..")
    graph_util.create_in_batch(graph, relationships)
    print("Done.")

def add_gender_label(graph,args):
    users = list(graph.nodes.match("User"))
    batches = [users[i:i + 5000] for i in range(0, len(users), 5000)]
    print("Adding %d gender labels.." % len(users))
    with tqdm(total=len(users)) as pbar:
        for batch in batches:
            tx = graph.begin()
            for user in batch:
                if (user['gender'] == "female") or (user['gender'] == "male"):
                    tx.merge(user)
                    if user['gender'] == "female":
                        user.add_label('Female')
                    else:
                        user.add_label('Male')
                    tx.push(user)
                pbar.update(1)
            tx.commit()
    print("Done.")

def add_relevant_label(graph,args):
    tweets = list(graph.nodes.match("Tweet"))
    batches = [tweets[i:i + 5000] for i in range(0, len(tweets), 5000)]
    print("Adding %d relevant labels.." % len(tweets))

    with tqdm(total=len(tweets)) as pbar:
        for batch in batches:
            tx = graph.begin()
            for tweet in batch:
                if tweet['date'] != None:
                    tx.merge(tweet)
                    date = datetime.strptime(tweet['date'], "%Y-%m-%d %H:%M:%S")
                    if ((datetime(2013,11,1) < date) and (date < datetime(2017, 11, 1))):
                        tweet.add_label('Relevant')
                    tx.push(tweet)
                pbar.update(1)
            tx.commit()
    print("Done.")

def remove_entities(graph, args):
    """ Removes all hashtags, mentions and links from all tweets
    to improve later analysis quality.
    """

    import re
    WEB_URL_REGEX = r"""(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"""

    def strip_entities(tweet):
        """ nested function, won't be used anywhere else anyways.
        """
        text = tweet['text']
        tweet['text_OLD'] = text # write backup copy of old text for annotation and stuff.
        for hashtag in tweet['hashtags']:
            text = text.replace('#'+hashtag, '')
        text = re.sub(WEB_URL_REGEX,'', text)
        # for media in tweet["media"]:
        text = re.sub('(@ \w*|@\w*)', '',text)
        tweet['text'] = text # save back
        return tweet

    query = "MATCH (t:Tweet) RETURN t;"
    responses = list(graph.run(query))
    batches = [responses[i:i + 5000] for i in range(0, len(responses), 5000)]
    with tqdm(total=len(responses)) as pbar:
        for batch in batches:
            tx = graph.begin()
            for record in batch:
                tweet = record['t']

                tx.merge(tweet)
                tweet = strip_entities(tweet)
                tx.push(tweet)

                pbar.update(1)
            tx.commit()


def write_nlp(graph, args):
    import spacy

    nlp = spacy.load('de_core_news_sm')
    query = "MATCH (u:User)-[:TWEETED]-(t:Relevant) RETURN t;"
    responses = list(graph.run(query))
    batches = [responses[i:i + 5000] for i in range(0, len(responses), 5000)]
    with tqdm(total=len(responses)) as pbar:
        for batch in batches:
            tx = graph.begin()
            for record in batch:
                tweet = record['t']
                processed = nlp(tweet['text'])
                tx.merge(tweet)
                tweet["tokens"] = [token.orth_ for token in processed]
                tweet["lemmata"] = [token.lemma_ for token in processed]
                tweet["pos"] = [token.pos_ for token in processed]
                tx.push(tweet)

                pbar.update(1)
            tx.commit()

def write_sentiment(graph, args):
    """computes the sentiment values.
    """
    import math
    # We will perform sentiment analysis on the lemmata, this might
    # reduce stuff but it's more practical
    def computeSentiment(senti_dict, tweet):
        sentence = tweet['lemmata']
        sentilist = []
        comparelist = [] # We use this to only get eindeutige sentivalues
        if isinstance(sentence, list):
            for token in sentence:
                if token in senti_dict.keys() and float(senti_dict[token]) != 0:
                    x = float(senti_dict[token]) # OP_VAL
                    sentilist.append(x)
                    rounded = math.ceil(x) if x > 0 else math.floor(x) # we round all values to -1, 1 or 0 for comparison
                    comparelist.append(rounded)
            if len(comparelist) == abs(sum(comparelist)): # will only be the same if only pos or only negs
                return sum(sentilist)
            else:
                return 0
        else:
            return 0

    def get_senti_SEPL():
        """ reads in SEPL sentiment dictionary
        """
        import csv
        senti_dict = {}

        #utf_8_encoder(self.p)
        with open("../senti_scripts/SePL.csv",encoding = 'utf-8', mode= "r") as csvfile:
            reader = csv.DictReader(csvfile, fieldnames = ('WORD', 'OP_VAL', 'STD_DEV', 'STD_ERR', 'PHR_TYP',"MAN_COR"), delimiter = ';')

            for row in reader:
                senti_dict[row['WORD']] = row['OP_VAL'] # reads this very simplified
        return senti_dict


    print("Writing sentiments..")
    query = "MATCH (t:Relevant) RETURN t;"
    responses = list(graph.run(query))

    senti_dict = get_senti_SEPL() # load sentiment


    batches = [responses[i:i + 5000] for i in range(0, len(responses), 5000)]
    with tqdm(total=len(responses)) as pbar:
        for batch in batches:
            tx = graph.begin()
            for record in batch:

                tweet = record['t']
                tx.merge(tweet)
                tweet['sentiment'] = computeSentiment(senti_dict, tweet)
                tx.push(tweet)

                pbar.update(1)
            tx.commit()


def write_sentiment_vader(graph, args):
    """computes the sentiment values.
    """

    senti = SentimentIntensityAnalyzer()

    print("Writing sentiments..")
    query = "MATCH (t:Relevant) RETURN t;"
    responses = list(graph.run(query))
    batches = [responses[i:i + 5000] for i in range(0, len(responses), 5000)]
    with tqdm(total=len(responses)) as pbar:
        for batch in batches:
            tx = graph.begin()
            for record in batch:

                tweet = record['t']
                tx.merge(tweet)
                sentiment = senti.polarity_scores(tweet['text'])['compound']
                tweet['sentiment'] = sentiment
                tx.push(tweet)

                pbar.update(1)
            tx.commit()

def annotate_gold_frames(graph, args):
    from py2neo import NodeMatcher
    matcher = NodeMatcher(graph)
    import csv
    # First reading in the annotated data into a dictionary.
    annotated = {}
    with open("../data/tweets_manual_annotated.csv", encoding= 'utf-8', mode='r') as f:
        csvfile = csv.DictReader(f, delimiter=';')
        for line in csvfile:
            try:
                frame = int(line['frames'])
                annotated[line['text_OLD']] = frame

            except Exception:
                continue
    # next step, write them to their respective tweets
    tx = graph.begin()
    print("Writing %d frame numbers to graph.." % len(annotated))
    counter = 0
    with tqdm(total=len(annotated)) as pbar:
        for tweet in annotated.keys():
            # query = "MATCH (t:Tweet) WHERE t.text_OLD = \"%s\" RETURN t;" % tweet
            # responses = list(graph.run(query))
            responses = list(matcher.match("Tweet", text_OLD=("%s" % tweet)))
            if len(responses) != 1:
                print("%d RESPONSES FOR TWEET %s" % (len(responses), tweet))
            else:
                node = responses[0]
                tx.merge(node)
                node['frame'] = annotated[tweet]
                tx.push(node)
                pbar.update(1)
                counter += 1

    tx.commit()
    print("DONE. %d/%d written." % (counter, len(annotated)))
