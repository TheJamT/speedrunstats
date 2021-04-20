import glob
import datetime
import json
from pymongo import MongoClient, errors
from pprint import pprint

client = MongoClient("mongodb://localhost")

DATE = '09' #ddmmyy

dbName = f"speedrun-{DATE}"
folderName = f'./stats/{DATE}/'

db = client[dbName]
print(client.server_info())

for folder in glob.glob(f'{folderName}/*'):
    for file in glob.glob('{}/*'.format(folder)):
        with open(file) as data:
            user = folder.split('/')[-1]
            timestamp = file.split('/')[-1].strip('.json')
            jsonData = json.load(data)
            jsonData['timestamp'] = datetime.datetime.fromtimestamp(int(timestamp))
            jsonData['_id'] = "{}_{}".format(user,timestamp)

            try:
                result = db[user].insert_one(jsonData)
                print("Created as {}".format(result.inserted_id))

            except errors.DuplicateKeyError:
                print("This record already exists, skipping.")

        # print("User: {}\tTimestamp: {}".format(user,datetime.datetime.fromtimestamp(int(timestamp)).strftime("%d/%m/%Y %H:%M")))