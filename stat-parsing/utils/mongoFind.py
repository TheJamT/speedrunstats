from pymongo import MongoClient
import datetime
import requests

client = MongoClient("mongodb://10.0.0.200")

db = client.minecraftBoisStats

with open("data.csv", "w") as outputFile:
    for collection in db.list_collection_names():
        username = requests.get("https://playerdb.co/api/player/minecraft/{}".format(collection)).json()["data"]["player"]["username"]
        print(username)
        for entry in (
            db[collection]
            .find({}, {"stats.minecraft:custom.minecraft:horse_one_cm": 1, "timestamp": 1})
            .sort("timestamp")
        ):
            mined = 0
            try:
                mined = entry["stats"]["minecraft:custom"]["minecraft:horse_one_cm"]
            except:
                mined = 0
            timestamp = entry["timestamp"]
            outputFile.write("{},{},{}\n".format(username,timestamp,mined))
            print("{}\t{}\t{}".format(username,timestamp, mined))
