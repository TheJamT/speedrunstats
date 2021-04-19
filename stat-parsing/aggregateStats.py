# Requires the PyMongo package.
# https://api.mongodb.com/python/current
import os
from pymongo import MongoClient
import requests
from datetime import datetime
import pandas as pd
import matplotlib as plt
import json

client = MongoClient("mongodb://localhost:27017/")

STAT_CATEGORY = "custom"  # eg: mined
STAT_NAME = "deaths"  # eg: stone
DATE = "170421"  # ddmmyy
SPEEDRUN_NO = 8
TITLE = "Total Walked" or f'{STAT_CATEGORY} {STAT_NAME}'

IGNORE_SPECTATORS = ['Daruksprotection']

results = {}
usernames = []

users = client[f"speedrun-{DATE}"].list_collection_names()


def getAggregateFunction():
    if STAT_NAME != "":
        return [
            {
                "$project": {
                    "_id": "$timestamp",
                    "total": f"$stats.minecraft:{STAT_CATEGORY}.minecraft:{STAT_NAME}",
                }
            },
            {"$sort": {"_id": 1}},
        ]

    else:
        return [
            {
                "$project": {
                    "_id": 0,
                    "timestamp": 1,
                    "stat": {"$objectToArray": f"$stats.minecraft:{STAT_CATEGORY}"},
                }
            },
            {"$unwind": {"path": "$stat"}},
            {"$group": {"_id": "$timestamp", "total": {"$sum": "$stat.v"}}},
            {"$sort": {"_id": 1}},
        ]


for user in users:
    if user == "system.views":
        continue


    getUsernameRequest = requests.get(
        f"https://playerdb.co/api/player/minecraft/{user}"
    )
    usernameRequestJSON = getUsernameRequest.json()
    username = usernameRequestJSON["data"]["player"]["username"]

    if username in IGNORE_SPECTATORS:
        continue

    usernames.append(username)

    result = client[f"speedrun-{DATE}"][user].aggregate(getAggregateFunction())
    result = list(result)

    results[username] = {}

    for resultItem in result:
        try:
            results[username][str(resultItem["_id"])] = resultItem["total"]
        except:
            results[username][str(resultItem["_id"])] = 0

dataTable = pd.read_json(json.dumps(results)).sort_index().interpolate('time')

dataTable.to_csv('test2.csv')

plot = dataTable.plot(title=f'Speedrun {SPEEDRUN_NO}: {TITLE}', ylabel=TITLE, xlabel="Timestamp", figsize=(8,5))
plot.legend(loc="best")
plot.grid(linewidth=0.25)
fig = plot.get_figure()
fig.savefig("output.png")
print(TITLE)
