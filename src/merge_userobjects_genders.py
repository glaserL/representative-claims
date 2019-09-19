import json
import pandas as pd
import re

from tqdm import tqdm

split_regex = re.compile("(-)|( )|(_)|(\.)") # filter regex to split multiple constituencies

gender_data = {}
gender_mapping = {}
with open("../data/users_with-loc.json", encoding='utf-8', mode='r') as f:
    inf = f.read()
    user_data = json.loads(inf)

with open("../data/name2gender.json", encoding = 'utf-8', mode='r') as f:
    inf = f.read()
    raw_gender_data = json.loads(inf)

# Create a nicer mapping name -> (gender, probability)
for response in gender_data:
    if "probability" in response.keys():
        gender_mapping[response['name']] = (response['gender'], response['probability'])


print("Annotating gender..")
for user in tqdm(user_data):
    firstname = split_regex.split(user['name'])[0]
    usergender = {}
    if firstname in gender_mapping.keys():
        usergender["name"] = gender_mapping[firstname][0]
        usergender["probability"] = gender_mapping[firstname][1]
    user['gender'] = usergender

with open("../data/users_with-loc_with-gender.json",  encoding='utf-8', mode='w') as f:
    json.dump(user_data, f, indent=2, ensure_ascii=False)
