import json


tweets = json.load(open("../data/tweets_fully_hydrated.json", encoding="utf-8"))

user_objects = []
for tweet in tweets:
    user_objects.append(tweet["user"])

unique_user_objects = []
print(f"Got {len(user_objects)}, will remove doublets.")
already_seen_ids = set()
for user_object in user_objects:
    if user_object["id"] in already_seen_ids:
        print(f"Detected doublet id {user_object['id']}, ignoring.")
        continue
    else:
        unique_user_objects.append(user_object)
        already_seen_ids.add(user_object["id"])

print(f"Writing {len(unique_user_objects)} to file.")

with open("../data/users_fully_hydrated.json", "w", encoding="utf-8") as outf:
    json.dump(unique_user_objects, outf, indent=2, ensure_ascii=False)
