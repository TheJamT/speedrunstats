# Requires the PyMongo package.
# https://api.mongodb.com/python/current
import os
from pymongo import MongoClient
import requests
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/")

STAT_CATEGORY = "custom"  # eg: mined
STAT_NAME = "deaths"  # eg: stone
DATE = "080421"  # ddmmyy

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
    usernames.append(username)

    result = client[f"speedrun-{DATE}"][user].aggregate(getAggregateFunction())

    result = list(result)

    results[username] = []

    for resultItem in result:
        try:
            results[username].append(
                {"time": resultItem["_id"], "stat": resultItem["total"]}
            )
        except:
            results[username].append({"time": resultItem["_id"], "stat": 0})

    # for resultItem in result:
    #     if resultItem["_id"] not in results:
    #         results[str(resultItem["_id"])] = {}
    #     try:
    #         results[str(resultItem["_id"])][username] = resultItem["total"]
    #     except:
    #         results[str(resultItem["_id"])][username] = 0


# firstTime = sorted(results)[0]

# if not os.path.exists(f"output/{DATE}"):
#     os.makedirs(f"output/{DATE}")

# with open(
#     f"output/{DATE}/{STAT_CATEGORY}-{STAT_NAME or 'total'}.csv", "w"
# ) as outputFile:
#     output = "duration,"
#     lastDataPoint = {}
#     for username in usernames:
#         output += f"{username},"
#         # lastDataPoint[username] = 0
#     output = output[:-1] + "\n"

#     for result in sorted(results):
#         timeDifference = round((datetime.strptime(result, '%Y-%m-%d %H:%M:%S') - datetime.strptime(firstTime, '%Y-%m-%d %H:%M:%S')).total_seconds() / 60.0, 1)
#         output += f"{timeDifference},"

#         for username in usernames:
#             try:
#                 dataPoint = results[result][username]
#                 output += f"{dataPoint},"
#                 # lastDataPoint[username] = dataPoint
#             except:
#                 output += ','
#                 # output += f"{lastDataPoint[username]},"
#         output = output[:-1] + "\n"

#     outputFile.write(output)


import json

with open("out.json", "w") as jsontemp:
    json = json.dumps(results, default=str)
    jsontemp.write(json)