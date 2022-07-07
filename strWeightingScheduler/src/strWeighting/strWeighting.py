# @package strWeighting
# strWeighting is a python script in charge of creating the weights from each strategy from every Coin and Pair 
# @author Angel Avalos

import sys
from typing import Collection
sys.path.insert(0, r'')

from pymongo import ASCENDING
import json
import time
import os
from strWeightingScheduler.src.utils.exceptions.exceptions import BadKwargs, SymbolNotSupported
import datetime
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import random
from strWeightingScheduler.src.utils.dbUtils.dbUtils import mongoDBClient
from strWeightingScheduler.src.utils.loggers.loggers import *


class strWeighting():
    def __init__(self):
        self.maxCoinPoints = 5
        self.maxTtlTrades = 15 
        self.maxPctPstTrades = 25
        self.maxAccIncrease = 45
        self.maxAvgTradeTime = 5
        self.maxTmstpLastTrade = 5

        self.minMaxNotDfnd = {'strPoints': False, 'strPoints_long': False, 'strPoints_short': False}

        self.min_real_ttlTrades = 50
        self.min_real_pctPstTrades = 20
        self.min_real_accIncrease = 5
        self.min_real_avgTradeTime = {'strPoints': None, 'strPoints_long': None, 'strPoints_short': None}
        self.min_real_tmstpLastTrade = {'strPoints': None, 'strPoints_long': None, 'strPoints_short': None}

        self.max_real_ttlTrades = 200
        self.max_real_pctPstTrades = 60
        self.max_real_accIncrease = 70
        self.max_real_avgTradeTime = {'strPoints': None, 'strPoints_long': None, 'strPoints_short': None}
        self.max_real_tmstpLastTrade = {'strPoints': None, 'strPoints_long': None, 'strPoints_short': None}

        self.strResults = {'strPoints': {}, 'strPoints_long': {}, 'strPoints_short': {}}
        intervals = {  # "5m": 300,
                "15m": 900,
                "30m": 1800,
                "1h": 3600,
                "2h": 7200,
                "4h": 14400,
                "1d": 86400}
        name = "strWeighting" 
        rPath = os.getcwd()
        dblPath = os.path.join(rPath, "tradingBot", "dataBase", name, "logs")
        if not os.path.exists(dblPath):
            os.makedirs(dblPath)
        debug_filename = (os.path.join(dblPath, "debug_{}.log".format(name)))
        self.debugLog = loggerFactory.setLogger("debugLogger", logFile=debug_filename, formatter='console', handler="fileHandler")
        self.debugLog.info("LOGGING IN DEV")

    def createWeighting(self):
        avlDB = self.AllDBs()
        self.analStrategies(avlDB)
        self.crtPoints()
        self.uploadPoints()
        self.debugLog.info("Finished strWeighting")  

    def analStrategies(self, avlDB):
        for coinPair in avlDB:
            self.DB.connectToDB(coinPair)
            collection = self.DB.getCollection('tradesInformation')
            if collection:
                self.debugLog.info("<<<CoinPair: {}>>>".format(coinPair))
                for strType in self.strResults.keys():
                    self.strResults[strType][coinPair] = {}
                rawData = self.DB.getDocuments(collection, order=ASCENDING)
                rawData = pd.DataFrame.from_dict(rawData, orient='columns')
                if 'strategyId' in rawData.columns:
                    
                    #rawData = rawData.drop_duplicates(subset=None, keep="first", inplace=False)
                    longData = rawData[rawData['typeOfTrade'] == 'Long'].reset_index(drop=True)
                    shortData = rawData[rawData['typeOfTrade'] == 'Short'].reset_index(drop=True)
                    data = {'strPoints': rawData, 'strPoints_long': longData, 'strPoints_short': shortData}
                    for strType in self.strResults.keys():
                        strIDs = data[strType]['strategyId'].unique()
                        for ID in strIDs:
                            self.strResults[strType][coinPair][ID] = {}
                            try:
                                testData = data[strType][data[strType]['isTest'] == True].reset_index(drop=True)
                                realData = data[strType][data[strType]['isTest'] != True].reset_index(drop=True)
                                dividedData = {'real': realData, 'test': testData}
                            except:
                                dividedData = {'real': data[strType]}
                            for val in dividedData.keys():
                                strTrades = dividedData[val][dividedData[val]['strategyId'] == ID].reset_index(drop=True)
                                if len(strTrades) > 0:
                                    ttlTrades, pctPstTrades, accIncrease, avgTradeTime, \
                                        tmstpLastTrade, coinPoints = self.analStrData(strTrades)
                                    strInformation = {
                                        'symbol': coinPair,
                                        'strategyId': ID,
                                        '{}_totalTrades'.format(val): ttlTrades,
                                        '{}_tradesEffectiveness'.format(val): pctPstTrades,
                                        '{}_accIncrease'.format(val): accIncrease,
                                        '{}_avgTradeTime'.format(val): avgTradeTime,
                                        '{}_tmstpLastTrade'.format(val): int(tmstpLastTrade),
                                        '{}_coinPoints'.format(val): coinPoints
                                    } 
                                    self.strResults[strType][coinPair][ID].update(strInformation)
                                    self.debugLog.info("Strategy ID {}: {}".format(val, ID))
                                    self.debugLog.info("Total trades: {}".format(ttlTrades))
                                    self.debugLog.info("% of positive trades: {:0.2f} %".format(pctPstTrades))
                                    self.debugLog.info("Acc Increase: {:0.2f} %".format(accIncrease))
                                    self.debugLog.info("Avg Time per Trade: {}".format(avgTradeTime))
                                    if not self.minMaxNotDfnd[strType] and val == "real": 
                                        self.minMaxNotDfnd[strType] = True
                                        self.min_real_avgTradeTime[strType] = avgTradeTime
                                        self.min_real_tmstpLastTrade[strType] = tmstpLastTrade

                                        self.max_real_avgTradeTime[strType] = avgTradeTime
                                        self.max_real_tmstpLastTrade = tmstpLastTrade
                                    elif val == "real":
                                        self.max_real_avgTradeTime[strType] = avgTradeTime if avgTradeTime > self.max_real_avgTradeTime[strType] else self.max_real_avgTradeTime[strType]
                                        self.min_real_avgTradeTime[strType] = avgTradeTime if avgTradeTime < self.min_real_avgTradeTime[strType] else self.min_real_avgTradeTime[strType]
                                        self.max_real_tmstpLastTrade = tmstpLastTrade if tmstpLastTrade > self.max_real_tmstpLastTrade else self.max_real_tmstpLastTrade
                                        self.min_real_tmstpLastTrade[strType] = tmstpLastTrade if tmstpLastTrade < self.min_real_tmstpLastTrade[strType] else self.min_real_tmstpLastTrade[strType]

    def crtPoints(self):
        for strType in self.strResults.keys():
            for coinPair in self.strResults[strType].keys():
                for ID in self.strResults[strType][coinPair].keys():
                    testStatus = True
                    realStatus = True
                    for val in ["real", "test"]:
                        try:
                            ttlTrades = self.strResults[strType][coinPair][ID]['{}_totalTrades'.format(val)]
                            pctPstTrades = self.strResults[strType][coinPair][ID]['{}_tradesEffectiveness'.format(val)]
                            accIncrease = self.strResults[strType][coinPair][ID]['{}_accIncrease'.format(val)]
                            avgTradeTime = self.strResults[strType][coinPair][ID]['{}_avgTradeTime'.format(val)]
                            tmstpLastTrade = self.strResults[strType][coinPair][ID]['{}_tmstpLastTrade'.format(val)]
                            coinPoints = self.strResults[strType][coinPair][ID]['{}_coinPoints'.format(val)]
                            pntCoinPoints, pntTtlTrades, pntPctPstTrades, pntAccIncrease, pntAvgTradeTime, \
                                pntTmstpLastTrade, ttlPoints = self.pntStr(ttlTrades, pctPstTrades, \
                                    accIncrease, avgTradeTime, tmstpLastTrade, coinPoints, strType)
                            strPointsResults = {
                                        '{}_points_coinPoints'.format(val): pntCoinPoints,
                                        '{}_points_totalTrades'.format(val): pntTtlTrades,
                                        '{}_points_tradesEffectiveness'.format(val): pntPctPstTrades,
                                        '{}_points_accIncrease'.format(val): pntAccIncrease,
                                        '{}_points_avgTradeTime'.format(val): pntAvgTradeTime,
                                        '{}_points_tmstpLastTrade'.format(val): pntTmstpLastTrade,
                                        '{}_ttlPoints'.format(val): ttlPoints,
                                        '{}_ttlPointsVsTrades': ttlPoints, 
                                        'totalPoints'.format(val): ttlPoints
                                    } 
                            self.strResults[strType][coinPair][ID].update(strPointsResults)
                        except:
                            testStatus = False if val == "test" else testStatus
                            realStatus = False if val == "real" else realStatus
                    if testStatus and realStatus:
                        realPoints = self.strResults[strType][coinPair][ID]['real_ttlPoints']
                        testPoints = self.strResults[strType][coinPair][ID]['test_ttlPoints']
                        realTtlTrd = self.strResults[strType][coinPair][ID]['test_totalTrades']
                        testTtlTrd = self.strResults[strType][coinPair][ID]['test_totalTrades']
                        dcrTestTtlTrd = (testTtlTrd / (np.log(realTtlTrd) + 1)) if realTtlTrd < 151 else 0
                        realPntVsTrds = (realPoints * realTtlTrd) / (realTtlTrd + dcrTestTtlTrd)
                        testPntVsTrds = (testPoints * dcrTestTtlTrd) / (realTtlTrd + dcrTestTtlTrd)
                        self.strResults[strType][coinPair][ID]['real_ttlPointsVsTrades'] = realPntVsTrds
                        self.strResults[strType][coinPair][ID]['test_ttlPointsVsTrades'] = testPntVsTrds
                        self.strResults[strType][coinPair][ID]['totalPoints'] = \
                            realPntVsTrds + testPntVsTrds

    def uploadPoints(self):
        for strType in self.strResults.keys():
            for coinPair in list(self.strResults[strType]):
                if len(self.strResults[strType][coinPair]) == 0:
                    del self.strResults[strType][coinPair]

        #with open("strResults/strWeightResults.json", 'w') as f:
        #    json.dump(self.strResults[strType], f, indent=1) 

        dataToDB = {'strPoints': [], 'strPoints_long': [], 'strPoints_short': []}
        for strType in self.strResults.keys():
            for coinPair in self.strResults[strType].keys():
                for ID in self.strResults[strType][coinPair].keys():
                    dataToDB[strType].append(self.strResults[strType][coinPair][ID])

        self.DB.connectToDB('General') 
        for strType in self.strResults.keys():
            self.DB.dropCollection(strType)
            self.DB.createCollection(strType, 'timestamp', unique=False)
            self.debugLog.info("Uploading Data to DB")     
            self.DB.insertDocuments(strType, dataToDB[strType])

    def analStrData(self, strTrades):
        positiveTrades = len(strTrades[strTrades['positiveTrade'] == True])
        avgTradeTime = strTrades['timeSinceTradeOpen'].mean()
        tmstpLastTrade = strTrades['closingTimestamp'].max()
        ttlTrades = len(strTrades)
        accIncrease = 1
        for i in range(0, ttlTrades):
            accIncrease = accIncrease * (1 + strTrades['percentageAccIncrease'].iloc[i])
        accIncrease = accIncrease * 100
        pctPstTrades = (positiveTrades / ttlTrades) * 100
        coinPoints = 10
        return ttlTrades, pctPstTrades, accIncrease, avgTradeTime, tmstpLastTrade, coinPoints

    def pntStr(self, ttlTrades, pctPstTrades, accIncrease, avgTradeTime, tmstpLastTrade, coinPoints, strType):
        pntCoinPoints = coinPoints
        pntTtlTrades = self.maxTtlTrades * (((100 * (ttlTrades - self.min_real_ttlTrades)) / (self.max_real_ttlTrades - self.min_real_ttlTrades)) / 100)
        pntPctPstTrades = self.maxPctPstTrades * (pctPstTrades / 100)
        pntAccIncrease = self.maxAccIncrease * (((100 * (accIncrease - self.min_real_accIncrease)) / (self.max_real_accIncrease - self.min_real_accIncrease)) / 100)
        pntAvgTradeTime = self.maxAvgTradeTime * (((100 * (avgTradeTime - self.max_real_avgTradeTime[strType])) / (self.min_real_avgTradeTime[strType] - self.max_real_avgTradeTime[strType])) / 100)
        pntTmstpLastTrade = self.maxTmstpLastTrade * (((100 * (tmstpLastTrade - self.min_real_tmstpLastTrade[strType])) / (self.max_real_tmstpLastTrade - self.min_real_tmstpLastTrade[strType])) / 100)

        pntCoinPoints = self.maxCoinPoints if pntCoinPoints > self.maxCoinPoints else pntCoinPoints
        pntTtlTrades = self.maxTtlTrades if pntTtlTrades > self.maxTtlTrades else pntTtlTrades
        pntPctPstTrades = self.maxPctPstTrades if pntPctPstTrades > self.maxPctPstTrades else pntPctPstTrades 
        pntAccIncrease = self.maxAccIncrease if pntAccIncrease > self.maxAccIncrease else pntAccIncrease 
        pntAvgTradeTime = self.maxAvgTradeTime if pntAvgTradeTime > self.maxAvgTradeTime else pntAvgTradeTime
        pntTmstpLastTrade = self.maxTmstpLastTrade if pntTmstpLastTrade > self.maxTmstpLastTrade else pntTmstpLastTrade
        
        pntTtlTrades = 0 if pntTtlTrades < 0 else pntTtlTrades
        pntTmstpLastTrade = 0 if pntTmstpLastTrade < 0 else pntTmstpLastTrade
        pntAvgTradeTime = 0 if pntAvgTradeTime < 0 else pntAvgTradeTime

        ttlPoints = pntCoinPoints + pntTtlTrades + pntPctPstTrades + pntAccIncrease + pntAvgTradeTime + pntTmstpLastTrade
        
        return pntCoinPoints, pntTtlTrades, pntPctPstTrades, pntAccIncrease, pntAvgTradeTime, pntTmstpLastTrade, ttlPoints

    def AllDBs(self):
        self.DB = mongoDBClient(self.debugLog)
        self.avlDB = self.DB.getExistingDB()[:-2]
        return self.avlDB


if __name__ == "__main__":
    points = strWeighting()
    points.createWeighting()