# Requires the PyMongo package.
# https://api.mongodb.com/python/current
import os
from pymongo import MongoClient
import requests
from datetime import datetime
import pandas as pd
import matplotlib as plt
import json
import glob

client = MongoClient("mongodb://localhost:27017/")

DATE = "170421"  # ddmmyy
SPEEDRUN_NO = 8

IGNORE_SPECTATORS = ['Daruksprotection']

def getAggregateFunction(stat_catgory, stat_name = ''):
    if stat_name != "":
        return [
            {
                "$project": {
                    "_id": "$timestamp",
                    "total": f"$stats.minecraft:{stat_catgory}.minecraft:{stat_name}",
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
                    "stat": {"$objectToArray": f"$stats.minecraft:{stat_catgory}"},
                }
            },
            {"$unwind": {"path": "$stat"}},
            {"$group": {"_id": "$timestamp", "total": {"$sum": "$stat.v"}}},
            {"$sort": {"_id": 1}},
        ]

def getUsers(ignore):
    usernames = {}

    for user in client[f"speedrun-{DATE}"].list_collection_names():
        if user == "system.views":
            continue

        getUsernameRequest = requests.get(
                f"https://playerdb.co/api/player/minecraft/{user}"
            )
        usernameRequestJSON = getUsernameRequest.json()
        username = usernameRequestJSON["data"]["player"]["username"]

        if username in ignore:
            continue

        usernames[user] = username


    return usernames



def generateGraph(users: dict, stat_category, stat_name = '',  graph_title = ''):
    graph_title = graph_title if graph_title != '' else f'{stat_category} {stat_name}' if stat_name != '' else f'{stat_category}'
    results = {}
    for user in users.keys():
        result = client[f"speedrun-{DATE}"][user].aggregate(getAggregateFunction(stat_category, stat_name))
        result = list(result)

        results[users[user]] = {}

        for resultItem in result:
            try:
                results[users[user]][str(resultItem["_id"])] = resultItem["total"]
            except:
                results[users[user]][str(resultItem["_id"])] = 0

    dataTable = pd.read_json(json.dumps(results)).sort_index().interpolate('time')

    plot = dataTable.plot(title=f'Speedrun {SPEEDRUN_NO}: {graph_title}', ylabel=graph_title, xlabel="Timestamp", figsize=(8,5))
    plot.legend(loc="best")
    plot.grid(linewidth=0.25)
    fig = plot.get_figure()

    if not os.path.exists(f'./stat-parsing/output/{DATE}/'):
        os.makedirs(f'./stat-parsing/output/{DATE}/') 
    fig.savefig(f"./stat-parsing/output/{DATE}/{graph_title.replace(' ', '_').lower()}.png")

STATS_TO_GRAPH = [
    {
        "category": "crafted",
        "title": "Total Items Crafted"
    }, {
        "category": "picked_up",
        "stat": "ender_pearl",
        "title": "Ender Pearls Picked Up"
    }, {
        "category": "custom",
        "stat": "deaths",
        "title": "Deaths"
    }, {
        "category": "killed",
        "title": "Mobs Killed"
    },{
        "category": "mined",
        "title": "Blocks Mined"
    }
]

users = getUsers(IGNORE_SPECTATORS)

for stat_to_graph in STATS_TO_GRAPH:
    category = stat_to_graph.get("category")
    stat = stat_to_graph.get("stat", "")
    title = stat_to_graph.get("title", "")

    generateGraph(users, category, stat, title)


FILE_FOLDER = f'stat-parsing/output/{DATE}/'
DISCORD_URL = "https://discord.com/api/webhooks/834150475737202757/F4yb_ch-sH_RbPU9VVA9dSc3jLNGEvCQCh3VK0QJda1OHA2utm-VKVvBTa3wV_Sgp2Zt"

files = {}

for file in glob.glob(f'{FILE_FOLDER}/*'):
    fileBinary = open(file, 'rb')
    filename = file.split('/')[-1]

    files[filename] = fileBinary

response = requests.post(DISCORD_URL, files=files)

