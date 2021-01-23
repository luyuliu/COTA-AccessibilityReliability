import BasicSolver
import sys
import os
import time
import multiprocessing
import copy
from pymongo import MongoClient
from datetime import timedelta, date, datetime
from math import sin, cos, sqrt, atan2, pi, acos
from tqdm import tqdm
import time as atime
client = MongoClient('mongodb://localhost:27017/')


class DijkstraSolver(BasicSolver.BasicSolver):
    def __init__(self, args):
        BasicSolver.BasicSolver.__init__(self)
        timestamp = args[0]
        walkingDistanceLimit = args[1]
        timeDeltaLimit = args[2]
        walkingSpeed = args[3]
        scooterSpeed = args[4]
        scooterDistanceLimit = args[5]
        isRealTime = args[6]
        isScooter = args[7]

        self.walkingDistanceLimit = walkingDistanceLimit
        self.scooterDistanceLimit = scooterDistanceLimit

        self.timestamp = timestamp
        self.timeDeltaLimit = timeDeltaLimit
        self.timeLimit = timeDeltaLimit + timestamp
        self.walkingSpeed = walkingSpeed
        self.scooterSpeed = scooterSpeed
        self.isRealTime = isRealTime
        self.walkingTimeLimit = walkingDistanceLimit/walkingSpeed
        self.isScooter = isScooter

        # Time
        self.curDateTime = datetime.fromtimestamp(timestamp)
        self.curDate = self.curDateTime.date()
        todayDate = self.curDate.strftime("%Y%m%d")
        self.curGTFSTimestamp = self.find_gtfs_time_stamp(self.curDate)

        # print("current GTFS timestamp", self.curGTFSTimestamp)

        # Mongo GTFS setup
        self.db_GTFS = client.cota_gtfs
        self.db_real_time = client.cota_real_time
        self.col_stops = self.db_GTFS[str(self.curGTFSTimestamp) + "_stops"]
        self.col_stop_times = self.db_GTFS[str(
            self.curGTFSTimestamp) + "_stop_times"]
        self.col_real_times = self.db_real_time["R" + todayDate]

        self.db_scooter = client.lime_location
        self.col_scooter = self.db_scooter[todayDate]

        self.db_access = client.cota_access

        self.rl_stops = list(self.col_stops.find({}))
        self.rl_stop_times = list(self.col_real_times.find({"time": {"$gt": timestamp, "$lt": self.timeLimit}})) if isRealTime else list(
            self.col_real_times.find({"scheduled_time": {"$gt": timestamp, "$lt": self.timeLimit}}))

        if self.rl_stop_times != None:
            if isRealTime:
                self.rl_stop_times.sort(key=self.sortrlStopTimesRealtime)
            else:
                self.rl_stop_times.sort(key=self.sortrlStopTimesSchedule)

        self.stopsDic = {}
        self.scooterDic = {}
        self.visitedSet = {}
        self.timeListByStop = {}
        self.timeListByTrip = {}
        self.arcsDic = {}

        for eachStop in self.rl_stops:
            self.stopsDic[eachStop["stop_id"]] = eachStop

        for eachTime in self.rl_stop_times:
            stopID = eachTime["stop_id"]
            tripID = eachTime["trip_id"]
            try:
                self.timeListByStop[stopID]
            except:
                self.timeListByStop[stopID] = []

            self.timeListByStop[stopID].append(eachTime)

            try:
                self.timeListByTrip[tripID]
            except:
                self.timeListByTrip[tripID] = []

            self.timeListByTrip[tripID].append(eachTime)
        

        for eachTripID, eachTrip in self.timeListByTrip.items():
            if len(eachTrip) < 2:
                continue
            for index in range(0, len(eachTrip)-1):
                generatingStopID = eachTrip[index]["stop_id"]
                receivingStopID = eachTrip[index + 1]["stop_id"]
                try:
                    self.arcsDic[generatingStopID]
                except:
                    self.arcsDic[generatingStopID] = {}

                try:
                    self.arcsDic[generatingStopID][receivingStopID]
                except:
                    self.arcsDic[generatingStopID][receivingStopID] = {}

                timeGen = eachTrip[index]["time"] if self.isRealTime else eachTrip[index]["scheduled_time"]
                timeRec = eachTrip[index +
                                   1]["time"] if self.isRealTime else eachTrip[index + 1]["scheduled_time"]

                try:
                    self.arcsDic[generatingStopID][receivingStopID][timeGen]
                except:
                    self.arcsDic[generatingStopID][receivingStopID][timeGen] = {
                        'time_gen': timeGen, 'time_rec': timeRec, 'bus_time': - timeGen + timeRec, "trip_id": eachTripID}

        self.count = 0
        # print("Initialization: Done!")

    def sortrlStopTimesSchedule(self, value):
        return value["scheduled_time"]

    def sortrlStopTimesRealtime(self, value):
        return value["time"]

    def calculateDistance(self, latlng1, latlng2):
        R = 6373
        lat1 = float(latlng1["stop_lat"])
        lon1 = float(latlng1["stop_lon"])
        lat2 = float(latlng2["stop_lat"])
        lon2 = float(latlng2["stop_lon"])

        theta = lon1 - lon2
        radtheta = pi * theta / 180
        radlat1 = pi * lat1 / 180
        radlat2 = pi * lat2 / 180

        dist = sin(radlat1) * sin(radlat2) + cos(radlat1) * \
            cos(radlat2) * cos(radtheta)
        try:
            dist = acos(dist)
        except:
            dist = 0
        dist = dist * 180 / pi * 60 * 1.1515 * 1609.344

        return dist

    def findClosestStop(self):
        minDistance = sys.maxsize
        closestStop = False
        for eachStop in self.rl_stops:
            eachStopID = eachStop["stop_id"]
            # print(minDistance, self.visitedSet[eachStopID]['time'])
            # if self.visitedSet[eachStopID]['time'] < minDistance:
            #     print(eachStopID, self.visitedSet[eachStopID]['visitTag'])
            if self.visitedSet[eachStopID]['time'] < minDistance and self.visitedSet[eachStopID]['visitTag'] == False:
                minDistance = self.visitedSet[eachStopID]['time']
                closestStop = eachStop
        return closestStop

    def getTravelTime(self, closestStopID, eachStopID, aStartTime):
        # With bus trip, can wait and bus or walk and scooter.
        # All methods are exclusive between two stops. Options:
        # 1. bus + wait. For all arcs.
        # 2. walk. 
        # 3. walk + scooter. (only for startStop arcs, aka first-mile, and last-mile)

        # Risk1: may incur meaningless transfers; but we are not controlling that anyway. Possible solution: control trip_id or transfer_count
        # Risk2: how to deal with last-mile scooters? Mimic the first version.
        # Risk3: how to distinguish the first-mile trip? if timestamp is the starttimestamp, propagate using scooter; if not, propagate using walking
        travelTime = {
            "generatingStopID": closestStopID,
            "receivingStopID": eachStopID,
            "time": sys.maxsize,
            "scooterTime": None,
            "walkTime": None,
            "busTime": None,
            "waitTime": None,
            "tripID": None,
            "scooterID": None,
            "increment": None,
            "tripType": None
        }

        closestStop = self.stopsDic[closestStopID]
        eachStop = self.stopsDic[eachStopID]

        # 1. Propagate with bus and wait. Not controlling transfer count. Select the most recent trip.
        # More greedy: if there is a bus trip, wait for that regardless of the expected saved time by walking.
        try:
            self.arcsDic[closestStopID][eachStopID]
        except:
            pass # Not bus trip
        else: 
            for eachIndex, eachTrip in self.arcsDic[closestStopID][eachStopID].items():
                if eachTrip["time_gen"] >= aStartTime:
                    recentBusTime = eachTrip["time_gen"]
                    travelTime["tripID"] = eachTrip["trip_id"]
                    travelTime["busTime"] = eachTrip["bus_time"]
                    travelTime["waitTime"] = recentBusTime - aStartTime
                    travelTime["walkTime"] = 0
                    travelTime["scooterTime"] = 0
                    travelTime["time"] = travelTime["busTime"] + travelTime["waitTime"]
                    travelTime["tripType"] = "bus"
                    break
            return travelTime
            
        # 2. Propagate with walk
        # No edge, can only walk/scooter
        if self.visitedSet[closestStopID]["lastTripType"] == "walk" or self.visitedSet[closestStopID]["lastTripType"] == "scooter": # cannot make two subsequent non-transit arcs.
            return travelTime
        dist = self.calculateDistance(self.stopsDic[closestStopID], self.stopsDic[eachStopID]) # Can be precalculated
        walkTime_walk = dist/self.walkingSpeed
        totalTime_walk = walkTime_walk

        if totalTime_walk > self.walkingTimeLimit:
            pass
        elif travelTime["time"] > totalTime_walk: # if no bus trip, travelTime["time"] should be maxsize; or, the bus trip is very slow, but very unlikely
            travelTime["busTime"] = 0
            travelTime["waitTime"] = 0
            travelTime["scooterTime"] = 0
            travelTime["walkTime"] = walkTime_walk
            travelTime["time"] = totalTime_walk
            travelTime["tripType"] = "walk"

        # 3. Propagate with walk and scooter (but only update for startStop)
        if self.isScooter:
            try:
                self.scooterDic[closestStopID]
            except:
                surroundingScootersList = list(self.col_scooter.find({"location": {"$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [float(closestStop["stop_lon"]), float(closestStop["stop_lat"])]
                    },
                    "$maxDistance": self.walkingDistanceLimit
                }},
                    "ts": {
                    "$gte": aStartTime,
                    # 120 is the update frequency. Which means, the users will only see the available scooters then. But when they finally got there, it may be unavailable.
                    "$lte": aStartTime + 120
                }
                }))
                self.scooterDic[closestStopID] = surroundingScootersList
            else:
                surroundingScootersList = self.scooterDic[closestStopID]
            
            scooterID = None
            increment = None
            if surroundingScootersList != []:
                totalTime_scooter = sys.maxsize
                for closestScooter in surroundingScootersList:
                    scooterLocation = {
                        "stop_lat": closestScooter["latitude"],
                        "stop_lon": closestScooter["longitude"]
                    }
                    walkDist_scooter = self.calculateDistance(scooterLocation, closestStop)
                    scooterDist_scooter = self.calculateDistance(scooterLocation, eachStop)
                    if scooterDist_scooter > closestScooter["meter_range"]: # If out of power
                        walkDist_scooter +=  scooterDist_scooter - closestScooter["meter_range"]
                        scooterDist_scooter = closestScooter["meter_range"]
                    if walkDist_scooter > self.walkingTimeLimit: # If out of walking limit
                        continue
                    if scooterDist_scooter > self.scooterDistanceLimit:
                        continue
                    scooterTime_scooter = scooterDist_scooter/self.scooterSpeed
                    walkTime_scooter = walkDist_scooter/self.walkingSpeed
                    if totalTime_scooter > scooterTime_scooter + walkTime_scooter:
                        totalTime_scooter = scooterTime_scooter + walkTime_scooter
                        scooterID = closestScooter["new_id"]
                        increment = totalTime_walk - totalTime_scooter
            
            travelTime["scooterID"] = scooterID
            travelTime["increment"] = increment
            
            if aStartTime == self.timestamp and totalTime_scooter < travelTime["time"]: # if the first-mile problem and the time is less, record it. Or else, just ignore it.
                travelTime["busTime"] = 0
                travelTime["waitTime"] = 0
                travelTime["scooterTime"] = scooterTime_scooter
                travelTime["walkTime"] = walkTime_scooter
                travelTime["time"] = totalTime_scooter
                travelTime["tripType"] = "scooter"
        
        return travelTime
    
    def addToTravelTime(self, originalStrucuture, closestStructure, travelTime, tempStartTimestamp):
        originalStrucuture["time"] = closestStructure["time"] + travelTime["time"]
        originalStrucuture["scooterTime"] = closestStructure["scooterTime"] + travelTime["scooterTime"]
        originalStrucuture["walkTime"] = closestStructure["walkTime"] + travelTime["walkTime"]
        originalStrucuture["busTime"] = closestStructure["busTime"] + travelTime["busTime"]
        originalStrucuture["waitTime"] = closestStructure["waitTime"] + travelTime["waitTime"]
        originalStrucuture["lastTripType"] = travelTime["tripType"]
        originalStrucuture["lastTripID"] = travelTime["tripID"]
        originalStrucuture["generatingStopID"] = travelTime["generatingStopID"]
        originalStrucuture["receivingStopID"] = travelTime["receivingStopID"]

        if travelTime["busTime"] == 0:
            originalStrucuture["transferCount"] += 1


        if tempStartTimestamp == self.timestamp:
            originalStrucuture["firstScooterID"] = travelTime['scooterID']
            originalStrucuture["firstScooterIncrement"] = travelTime["increment"]
        else:
            originalStrucuture["lastScooterID"] = travelTime['scooterID']
            originalStrucuture["lastScooterIncrement"] = travelTime["increment"]
            originalStrucuture["firstScooterID"] = closestStructure['firstScooterID']
            originalStrucuture["firstScooterIncrement"] = closestStructure["firstScooterIncrement"]
        
        
        

        return originalStrucuture

    # Dijkstraly find shortest time to each stop.
    def extendStops(self, startStopID):
        startTimestamp = self.timestamp
        self.visitedSet = {}
        for eachStop in self.rl_stops: # Initialization
            eachStopID = eachStop["stop_id"]
            self.visitedSet[eachStopID] = {
                "startStopID": startStopID,
                "time": sys.maxsize,
                "scooterTime": 0,
                "walkTime": 0,
                "busTime": 0,
                "waitTime": 0,
                "generatingStopID": None,
                "receivingStopID": None,
                "lastTripID": None,
                "lastTripType": None,
                "transferCount": 0,
                "firstScooterID": None,
                "firstScooterIncrement": None,
                "lastScooterID": None,
                "lastScooterIncrement": None,
                "visitTag": False
            }

        self.visitedSet[startStopID]["time"] = 0 # initialization

        for _ in tqdm(self.rl_stops):
            closestStop = self.findClosestStop()
            if closestStop == False:
                print("break!")
                break
            closestStopID = closestStop["stop_id"]

            self.visitedSet[closestStopID]["visitTag"] = True

            # Can modify self.rl_stops to improve performance.

            for eachStop in (self.rl_stops):
                eachStopID = eachStop["stop_id"]
                tempStartTimestamp = startTimestamp + self.visitedSet[closestStopID]["time"]
                travelTime = self.getTravelTime(closestStopID, eachStopID, tempStartTimestamp)
                # if travelTime["tripType"] != None:
                #     print(self.visitedSet[eachStopID]["time"] > self.visitedSet[closestStopID]["time"] + travelTime["time"])
                if travelTime["time"] == None:
                    continue
                if travelTime['time'] > 0 and self.visitedSet[eachStopID]["visitTag"] == False and self.visitedSet[eachStopID]["time"] > self.visitedSet[closestStopID]["time"] + travelTime["time"]:
                    self.visitedSet[eachStopID] = self.addToTravelTime(self.visitedSet[eachStopID], self.visitedSet[closestStopID], travelTime, tempStartTimestamp)
                    # print("changed: ", self.visitedSet[eachStopID])
            # print(_["stop_id"], "finished!")
            # break
        self.insertResults(startStopID)
        # print(self.visitedSet)
        return self.visitedSet

    def insertResults(self, startStopID):
        ID = 'scooter' if self.isScooter else 'normal'
        col_access = self.db_access["abtest_" + ID + "_" + str(int(self.timestamp))]
        accessibleStops = self.visitedSet
        print("--------------------------------------------------------")
        col_access.drop()
        for eachStopIndex, eachStopValue in accessibleStops.items():
            if eachStopValue["lastTripType"] != None:
                stopID = eachStopValue["receivingStopID"]
                eachStopValue["stop_lat"] = self.stopsDic[stopID]["stop_lat"]
                eachStopValue["stop_lon"] = self.stopsDic[stopID]["stop_lon"]
                col_access.insert_one(eachStopValue)
        
        col_access.create_index([("startStopID", 1)])


def singleAccessibilitySolve(args, startLocation):
    # print(args, startLocation)
    accessibilitySolver = DijkstraSolver(args)
    lenStops = accessibilitySolver.extendStops(startLocation)
    return lenStops

def collectiveAccessibilitySolve(args, sampledStopsList):
    pool = multiprocessing.Pool(processes=31)
    output = []
    output = pool.starmap(singleAccessibilitySolve, zip([args]*len(sampledStopsList), sampledStopsList))
    pool.close()
    pool.join()
    return output

if __name__ == "__main__":
    basicSolver = BasicSolver.BasicSolver()
    # startDate = date(2019, 6, 20)
    startDate = date(2019, 6, 30)
    endDate = date(2019, 12, 18)
    walkingDistanceLimit = 700
    timeDeltaLimit = 30 * 60
    walkingSpeed = 1.4
    scooterSpeed = 4.47  # 10 mph
    scooterDistanceLimit = (5-1)/0.32*4.47*60 # 5 dollar 
    sampleRate = 50
    isRealTime = True
    isScooter = False
    daterange = (basicSolver.daterange(startDate, endDate))

    for singleDate in (daterange):
        GTFSTimestamp = basicSolver.find_gtfs_time_stamp(singleDate)
        todaySeconds = atime.mktime(singleDate.timetuple())
        # dbStops = client.cota_gtfs[str(GTFSTimestamp) + "_stops"]
        # allStopsList = dbStops.find({})
        # sampledStopsList = []
        # sampler = 0
        # for eachStops in allStopsList:
        #     if sampler % sampleRate == 1:
        #         sampledStopsList.append(eachStops["stop_id"])
        #     sampler += 1

        sampledStopsList = ['MOREASS', 'HIGCOON', '4TH15TN', 'TREZOLS', 'KARPAUN', 'LIVGRAE', 'GRESHEW', 'MAIOHIW', 'AGL540W', 'WHIJAEE', '3RDCAMW', 'HARZETS', 'MAIBRICE', 'SAI2NDS', '3RDMAIS', 'STYCHAS', 'LOC230N', 'BETDIEW', 'STEMCCS', 'INNWESE', 'HANMAIN', 'HIGINDN', '4THCHIN', 'RIDSOME', 'KARHUYN', 'LIVBURE', 'LONWINE', 'MAICHAW', 'BROHAMIW', 'WHI3RDE', '1STLINW', 'MAINOEW', 'MAIIDLE', '5THCLEE', '3RDTOWS', 'STYGAMS', 'KOE113W', 'TAM464S', 'CAS150S', 'BROOUTE', 'ALUGLENS', 'FRABREN', 'SOU340N', 'HILTINS', 'STRHOVE', 'SAWCOPN', 'HAMWORN', 'DALDUBN', 'MCNCHEN', 'HILBEAS', 'NOROWEN', 'SOUTER2A', 'GENSHAN', 'VACLINIC', 'MORHEATE', 'KOEEDSW1', 'TRAMCKW', 'FAISOUN', 'SAWSAWN', 'CLIHOLE', 'CHAMARN', 'CLE24THN']
        
        print("Date: ", singleDate, "; Stops: ", len(sampledStopsList))
        # todayTimestampList = [todaySeconds + 28800, todaySeconds + 46800, todaySeconds + 64800]
        todayTimestampList = [todaySeconds + 64800]
        for eachTimestamp in todayTimestampList:
            # if eachTimestamp != 1563192000: # debug and restart
            #     continue
            args = [int(eachTimestamp), walkingDistanceLimit, timeDeltaLimit, walkingSpeed, scooterSpeed, scooterDistanceLimit, isRealTime, isScooter]
            # resultsFeedback = collectiveAccessibilitySolve(args, sampledStopsList)

            testStopID = "3RDMAIS"
            resultsFeedback = singleAccessibilitySolve(args, testStopID)
            
            # print("eachTimestamp:", int(eachTimestamp), "results lens: ", resultsFeedback)
            print("--------------------------------------------------------------------------------")
            break
        break
            