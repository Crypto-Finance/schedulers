from typing import Dict
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import BulkWriteError
import pymongo
import os
import json

from bson import json_util
from pathlib import Path
from dotenv import load_dotenv

from strWeightingScheduler.src.utils.loggers.loggers import *

load_dotenv(str(Path().cwd()) + '/.env')
DB_PSW = os.getenv("DB_PASSWORD")
CB_DB_USER = os.getenv("CB_DB_USER")


class mongoDBClient():

    def __init__(self, debugLog):
        
        #CONNECTION_STRING = "mongodb+srv://dbAdmin:2TheMoon21@trdbotcluster.a777u.mongodb.net/myFirstDatabase"
        CONNECTION_STRING = "mongodb+srv://{}:{}@trdbotcluster.a777u.mongodb.net/myFirstDatabase".format(CB_DB_USER, DB_PSW)
        #Connect to a client
        self.client = MongoClient(CONNECTION_STRING)
        
        self.debugLog = debugLog.bind(module=self.__class__.__name__)
        
    def connectToDB(self, dbName):
        ##
        #@fn connectToDB
        #@param dbName 
        #@brief creates a connection to a specific DB, if not existant then creates it
        
        existingDBs = self.client.list_database_names()

        if(dbName not in existingDBs):
            self.debugLog.info("Creating database", symbol=dbName)
            self.db = self.client.get_database(dbName)
        else:
            self.debugLog.info("Database connection succesfull", symbol=dbName)
            self.db = self.client[dbName]       
    
    def createCollection(self, collectionName, index=None, **kwargs):
        
        ##
        #@fn createCollection
        #@param collectionName name of collection
        #@param index index in the collection
        #@brief creates a new collection and assign the index passed
       
        self.debugLog.info("creating Collection {}".format(collectionName), collection=collectionName)
        try:
            collection = self.db.create_collection(collectionName)
            
            self.createIndex(collection, index, **kwargs)
            
            self.debugLog.debug("Collection {} Created".format(collectionName))
            return collection
        except pymongo.errors.CollectionInvalid:
            self.debugLog.warning("Collection exists", collection=collectionName)
            return False
        
    def getExistingDB(self):
        ##
        #@fn getExistingDB
        #@brief returns a list of all available dbs in connection string
        
        existingDBs = list(self.client.list_database_names())
        
        return existingDBs
    
    def getCollection(self, collectionName):

        cursor = self.db.list_collections()
        
        if(collectionName not in [col['name'] for col in cursor]):
            self.debugLog.warning("Collection non existant")
            return False
        
        collection = self.db.get_collection(collectionName)
        self.debugLog.info("Collection found", collection=collectionName)
        
        return collection

    def getAllCollections(self):
        
        collections = self.db.list_collections_names()
        
        return collections
    
    def dropCollection(self, collectionName):
              
        return self.db.drop_collection(collectionName)
    
    def createIndex(self, collection, indexName, **kwargs):
        self.debugLog.info("Creating index {} for Collection {}".format(indexName, collection), collection=collection.name)
        collection.create_index(indexName, **kwargs)
        self.debugLog.info("Index Created", index=indexName)
        return True
        
    def getDocuments(self, collection, timestamp=None, order=ASCENDING):
              
        if timestamp:
            data = collection.find({'timestamp': timestamp})
        else:

            data = collection.find().sort('timestamp', order)
          
        return list(data)
    
    def replaceDocuments(self, collection, data):
        if isinstance(collection, str):
            collection = self.getCollection(collection)
        
        #only one candle to update
        if isinstance(data, dict):
            try:
                preDoc = collection.find_one_and_replace({'timestamp': data['timestamp']}, data)
                self.debugLog.debug("Before Doc: {} After Doc {}".format(preDoc, data))
            except Exception:
                self.debugLog.exception("Exception while updating Document")
        else:
            #TODO REPLACE SEVERAL DOCS
            pass
            
    def insertDocuments(self, collection, data):
        error = 0
        if isinstance(collection, str):
            collection = self.getCollection(collection)
        
        self.debugLog.info("Inserting documents", collection=collection.name)

        if isinstance(data, dict):
            try:
                res = collection.insert_one(data)
                self.debugLog.debug('Document inserted with ID {}'.format(res.inserted_id))
            except Exception as ex:
                self.debugLog.warning("Exception when writing one document {}".format(ex))
                error = 1
            
        elif isinstance(data, list):
            try:
                res = collection.insert_many(data)
                self.debugLog.debug('Document inserted with ID {}'.format(res.inserted_ids))

            except BulkWriteError:
                self.debugLog.exception("Error ocurred, check data to be inserted")
                error = 1
        self.debugLog.debug("Parsing data ObjectId to JSON serialized")
        data = json.loads(json_util.dumps(data))
        if error:        
            return False
        else:
            return res.acknowledged
            
                        
def convertLocal(filename):
    path = os.path.join(os.getcwd(), 'tradingBot', 'dataBase', 'BTC', 'USDT', 'intervals', filename)
    with open(path, 'r') as f:
        data = json.load(f)

    candles = data['data']
    
    return candles
          

# Th
# is is added so that many files can reuse the function get_database()
if __name__ == "__main__":    

    candles = convertLocal('1h.json')

    # Get the database
    dbObj = mongoDBClient("BTCUSDT")

    #GET COLLECTION
    collection = dbObj.getCollection('1h')
    dat = dbObj.getDocuments(collection)
    dat
    #INSERT IN COLLECTION
    #dbObj.insertDocuments(collection, candles)
    
    dat = dbObj.getDocuments(collection)
    dat
    #CREATE COLLECTION
    #dbObj.createCollection('1s')
    
    #collection = dbObj.getCollection('1d')
    #dbObj.createIndex(collection, 'timestamp')

    