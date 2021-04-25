from flask import Flask
from flask.wrappers import Response
from pymongo import MongoClient, errors as MongoErrors
from datetime import datetime
import glob
import json
import time
from watchdog import observers
import requests
import os
import pandas as pd
import matplotlib as plt

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler


SPEEDRUN_DATE_FORMATTED = datetime.now().strftime("%d/%m/%Y")
MINECRAFT_DIRECTORY = '/minecraft'
DATE = datetime.now().strftime("%d%m%y")
DISCORD_URL = '***REMOVED***'
IGNORE_SPECTATORS = []

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

client = MongoClient('mongodb://172.17.0.1')

db = client[f"speedrun-{DATE}"]

app = Flask(__name__)

# Creates the required mongodb aggregation
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

# Gets usernames from playerdb
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

    plot = dataTable.plot(title=f'Speedrun {SPEEDRUN_DATE_FORMATTED}: {graph_title}', ylabel=graph_title, xlabel="Timestamp", figsize=(8,5))
    plot.legend(loc="best")
    plot.grid(linewidth=0.25)
    fig = plot.get_figure()

    if not os.path.exists('./output'):
        os.makedirs('./output') 
    fig.savefig(f"./output/{graph_title.replace(' ', '_').lower()}.png")


@app.route('/finish')
def generateStatGraphs():
  users = getUsers(IGNORE_SPECTATORS)

  for stat_to_graph in STATS_TO_GRAPH:
    category = stat_to_graph.get("category")
    stat = stat_to_graph.get("stat", "")
    title = stat_to_graph.get("title", "")

    generateGraph(users, category, stat, title)

  graphFiles = {}

  for file in glob.glob('./output/*'):
    fileBinary = open(file, 'rb')
    filename = file.split('/')[-1]

    graphFiles[filename] = fileBinary

  
  response = requests.post(DISCORD_URL, files=graphFiles)


  return Response(status=200)



# Import stat runs everytime a file is modified, and saves it in mongodb
def importStat(event):
  userId = event.src_path.split('/')[-1].split('.')[0]
  filename, fileextension = os.path.splitext(event.src_path)

  print("DETECTEDCHANGE")
  print(event.src_path)

  dataRaw = ''
  attemptCount = 0

  while dataRaw == '':
    attemptCount += 1
    with open(event.src_path) as data:
      dataRaw = data.read()
    
    if attemptCount == 20:
      return None

  if(dataRaw[0] == "{"):
    jsonData = json.loads(''.join(dataRaw))
    currentTime = int(time.time())
    jsonData['timestamp'] = datetime.fromtimestamp(int(currentTime))
    jsonData['_id'] = "{}_{}".format(userId,currentTime)

    try:
        result = db[userId].insert_one(jsonData)
        print("Inserted as {}".format(result.inserted_id))

    except MongoErrors.DuplicateKeyError:
        print("This record already exists, skipping.")


# This sets up the event handler
def statCheckSetup():
  event_handler = PatternMatchingEventHandler("*", "", True, True)

  event_handler.on_created = importStat
  event_handler.on_modified = importStat

  path = f"{MINECRAFT_DIRECTORY}/world/stats"
  observer = Observer()
  observer.schedule(event_handler, path)  

  observer.start()

statCheckSetup()

