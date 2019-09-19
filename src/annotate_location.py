import json
import re
from tqdm import tqdm
userdata = []

with open("../data/state_synsets.txt", encoding = 'utf-8', mode = 'r') as f:
    state_mapping = json.load(f)


city_mapping = {}

find_state = re.compile("(\(\w{2}\))")
find_city = re.compile("(.+ )")
with open("../data/german_cities_raw.txt", encoding = 'utf-8', mode='r') as f:
    for entry in f:
        city = find_city.findall(entry)[0].strip()
        state = find_state.findall(entry)[0].replace("(","").replace(")","")
        city_mapping[city] = state
        # city_mapping[city] = ""

def is_german_city(x):
    if (not any(city.lower() in x.lower() for city in city_mapping.keys()) and
    not any(state.lower() in x.lower() for state in state_mapping.keys())):
        print(x)


def map_cities(location):
    matches = []
    for city in city_mapping.keys():
        if city in location:
            matches.append(city)
    return list(set(matches))
    # matches = [city.lower() in location for city in city_mapping.keys()]

def map_states(location):
    matches = []
    for city in city_mapping.keys():
        if city in location:
            matches.append(city_mapping[city])
    for state in state_mapping.keys():
        if state in location:
            matches.append(state_mapping[state])
    return list(set(matches))


with open("../data/users_fully_hydrated.json", encoding='utf-8', mode='r') as f:
    inf = f.read()
    userdata = json.loads(inf)

result = []
print("Annotating location..")
for user in tqdm(userdata):
    user['state'] = map_states(user['location'])
    user['city'] = map_cities(user['location'])
    result.append(user)

print("Done.")
with open("../data/users_with-loc.json",  encoding = 'utf-8', mode='w') as f:
    json.dump(result,f ,indent = 2,ensure_ascii=False)
