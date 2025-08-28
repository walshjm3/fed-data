import json

with open("cookies.json") as f:
    data = json.load(f)

print("COOKIES='" + json.dumps(data) + "'")

