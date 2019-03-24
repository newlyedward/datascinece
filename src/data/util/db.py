import pandas as pd
from pymongo import MongoClient

from src.data.setting import MONGODB_URI, MONGODB_PORT
from src.log import LogHandler

log = LogHandler('db.log')

# client = pymongo.MongoClient("mongodb+srv://unistar:<password>
# @cluster0-y9smy.azure.mongodb.net/test?retryWrites=true")
# db = client.test


def connect_mongo(db, username='', password='', host=MONGODB_URI, port=MONGODB_PORT):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://{}:{}@{}:{}/{}'.format(username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[db]


def read_mongo(database, collection, query={}, host=MONGODB_URI, port=MONGODB_PORT, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = connect_mongo(host=host, port=port, username=username, password=password, db=database)

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(cursor))

    # Delete the _id
    if no_id:
        del df['_id']

    return df


def to_mongo(database, collection, data: dict, host=MONGODB_URI, port=MONGODB_PORT, username=None, password=None):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = connect_mongo(host=host, port=port, username=username, password=password, db=database)

    # specific DB and Collection
    cursor = db[collection]

    length = len(data)

    if length == 0:
        return False
    elif length == 1:
        result = cursor.insert_one(data)
    else:
        result = cursor.insert_many(data)

    return result.acknowledged
