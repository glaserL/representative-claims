import json
import re
import requests
import random
from tqdm import tqdm
import json
API_KEY = json.load(open("config.json"))["genderize"]["apikey"]

global REMAINING
REMAINING = 100
CONTINUE = False

def constructRequest(names):
    request = []
    base = "https://api.genderize.io/?"
    lang = "&language_id=de"
    request.append(base)
    firstnames = []
    for i in range(len(names)):
        firstnames.append("name[%d]=%s"%(i, names[i]))
    request.append('&'.join(firstnames))
    request.append(lang)
    request.append(f"&apikey={API_KEY}")
    return {"length": len(names),
    "request": "".join(request)}

users = []

split_regex = re.compile("(-)|( )|(_)|(\.)") # filter regex to split multiple constituencies

outfile = "assume_gender.json"

with open("../data/users_fully_hydrated.json", encoding='utf-8', mode='r') as f:
    inf = f.read()
    users = json.loads(inf)


firstnames = []
for user in users:
    firstnames.append(split_regex.split(user['name'])[0])

if CONTINUE:
    status_quo = []

    with open(outfile, encoding='utf-8', mode='r') as f:
        inf = f.read()
        status_quo = json.loads(inf)

    already_assumed_genders = [name['name'] for name in status_quo]
    firstnames = set(firstnames) - set(already_assumed_genders)
    print("Removed %d already crawled usernames. (%d remaining.)" % (len(set(already_assumed_genders)), len(firstnames)))

print("Starting to assume genders of %d unique names.." % (len(firstnames)))
firstnames = list(firstnames) # back to list

queries = [constructRequest(firstnames[i:i+10]) for i in range (0, len(firstnames), 10)] # build queries from uniquyfied

def sendRequest(query):
    global REMAINING
    if query["length"] > REMAINING:
        print("Unsufficient credits to query %d names." % query["length"])
    else:
        response = requests.get(query["request"])
        REMAINING = int(response.headers['x-rate-limit-remaining'])
    return response.json()


with tqdm(total = len(firstnames)) as pbar:
    with open(outfile, encoding = 'utf-8', mode = 'w') as json_out:
        json_out.write('[') # result should be a list
        for query in queries:
            response = sendRequest(query)
            for assumption in response:
                json_out.write(json.dumps(assumption, indent=2, ensure_ascii=False)+',')
            json_out.flush()
            pbar.update(query["length"])
        json_out.write(']') # end list
        json_out.flush()

print("Done assuming genders for %d names, writing to %s." % (len(firstnames), outfile))
