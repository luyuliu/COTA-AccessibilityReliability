import sys
import os
import time, math
import multiprocessing
import copy
from pymongo import MongoClient
from datetime import timedelta, date, datetime
from math import sin, cos, sqrt, atan2, pi, acos
from tqdm import tqdm
from collections import OrderedDict
import time as atime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time

def revisit():
    db_access = client.cota_access_rel
    startDate = date(2018, 2, 1)
    endDate = date(2020, 7, 1)
    walkingDistanceLimit = 700
    timeDeltaLimit = 150 * 60
    walkingSpeed = 1.4
    sampleRate = 20
    daterange = (transfer_tools.daterange(startDate, endDate))

    for singleDate in (daterange):
        todayDate = singleDate.strftime("%Y%m%d")
        GTFSTimestamp = transfer_tools.find_gtfs_time_stamp(singleDate)
        todaySeconds = atime.mktime(singleDate.timetuple())
        gtfsSeconds = str(transfer_tools.find_gtfs_time_stamp(singleDate))
        todayTimestampList = []

        for i in [8, 12, 18]:
            todayTimestampList.append(todaySeconds + i* 60*60)
        
        for eachTimestamp in todayTimestampList:
            col_stops = db_GTFS[str(GTFSTimestamp) + "_stops"]
            col_stop_times = db_GTFS[str(GTFSTimestamp) + "_stop_times"]
            col_real_times = db_real_time["R" + todayDate]
            rl_stop_times_rt = list(col_real_times.find({"time": {"$gt": eachTimestamp, "$lt": eachTimestamp + timeDeltaLimit}})) # Real-time

            
            stopsDic = {}
            timeListByStopRT = {}
            timeListByTripRT = {}
            arcsDicRT = {}
                
            for eachTime in rl_stop_times_rt:
                stopID = eachTime["stop_id"]
                tripID = eachTime["trip_id"]
                try:
                    timeListByStopRT[stopID]
                except:
                    timeListByStopRT[stopID] = []

                timeListByStopRT[stopID].append(eachTime)

                try:
                    timeListByTripRT[tripID]
                except:
                    timeListByTripRT[tripID] = []

                timeListByTripRT[tripID].append(eachTime)

            for eachTripID, eachTrip in timeListByTripRT.items():
                if len(eachTrip) < 2:
                    continue
                for index in range(0, len(eachTrip)-1):
                    generatingStopID = eachTrip[index]["stop_id"]
                    receivingStopID = eachTrip[index + 1]["stop_id"]
                    try:
                        arcsDicRT[generatingStopID]
                    except:
                        arcsDicRT[generatingStopID] = {}

                    try:
                        arcsDicRT[generatingStopID][receivingStopID]
                    except:
                        arcsDicRT[generatingStopID][receivingStopID] = {}

                    timeGen = eachTrip[index]["time"]
                    timeRec = eachTrip[index + 1]["time"]

                    try:
                        arcsDicRT[generatingStopID][receivingStopID][timeGen]
                    except:
                        arcsDicRT[generatingStopID][receivingStopID][timeGen] = {
                            'time_gen': timeGen, 'time_rec': timeRec, 'bus_time': - timeGen + timeRec, "trip_id": eachTripID}

            # Sort the arcsDics with OrderedDic.
            for startStopID, startStop in arcsDicRT.items():
                for endStopID, endStop in startStop.items():
                    arcsDicRT[startStopID][endStopID] = OrderedDict(sorted(endStop.items()))

            accessDic = {}
            col_access = db_access[todayDate + "_" + str(eachTimestamp)]
            rl_access = col_access.find({})

            for eachOD in rl_access:
                originStopID = eachOD["startStopID"]
                destinationStopID = eachOD["receivingStopID"]
                if destinationStopID == None:
                    continue
                try:
                    accessDic[originStopID]
                except:
                    accessDic[originStopID] = {}
                
                try:
                    accessDic[originStopID][destinationStopID]
                except:
                    eachOD["revisitTag"] = False
                    accessDic[originStopID][destinationStopID] = eachOD

            for originStopID, ODs in accessDic.items():
                for destinationStopID, eachOD in ODs.items():
                    lastStopID = destinationStopID
                    
                    trajectoryList= [] # Store the stopIDs that have not been revisited along the trajectory 
                    while(lastStopID != originStopID):
                        thisRevisitTag = accessDic[originStopID][lastStopID]["revisitTag"]
                        if thisRevisitTag == True: # the previous stop has been revisited.
                            break
                        trajectoryList.insert(0, lastStopID)
                        lastStopID = accessDic[originStopID][lastStopID]["generatingStopIDSC"] # Move to the prior stop
                    
                    for intermediateStopID in trajectoryList:
                         
                