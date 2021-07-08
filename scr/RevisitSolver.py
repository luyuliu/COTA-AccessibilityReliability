
import sys
import os
import time
import math
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


def revisit(lastMiddleStopID, thisMiddleStopID, originStopID, eachTimestamp, accessDic, arcsDicRT):
    # Revisited Stop's time
    lastTimeRV = accessDic[originStopID][lastMiddleStopID]["timeRV"]
    if lastTimeRV == False: # There is no path to the last stop, so the rest of the path will be nonexisting.
        accessDic[originStopID][thisMiddleStopID]["timeRV"] = False
        accessDic[originStopID][thisMiddleStopID]["waitTimeRV"] = False
        accessDic[originStopID][thisMiddleStopID]["busTimeRV"] = False
        accessDic[originStopID][thisMiddleStopID]["lastTripIDRV"] = False
        return False
    lastTime = lastTimeRV + eachTimestamp
    # Should be always consistent with RV
    thisTripTypeSC = accessDic[originStopID][thisMiddleStopID]["lastTripTypeSC"]
    # lastTripTypeSC = accessDic[originStopID][lastMiddleStopID]["lastTripTypeSC"]

    accessDic[originStopID][thisMiddleStopID]["walkTimeRV"] = accessDic[originStopID][lastMiddleStopID]["walkTimeSC"]

    if thisTripTypeSC == "walk":
        accessDic[originStopID][thisMiddleStopID]["timeRV"] = accessDic[originStopID][lastMiddleStopID]["timeSC"]
        accessDic[originStopID][thisMiddleStopID]["waitTimeRV"] = 0
        accessDic[originStopID][thisMiddleStopID]["busTimeRV"] = 0
    else:  # "bus"
        try:
            arcsList = arcsDicRT[lastMiddleStopID][thisMiddleStopID]
        except:
            accessDic[originStopID][thisMiddleStopID]["timeRV"] = False
            accessDic[originStopID][thisMiddleStopID]["waitTimeRV"] = False
            accessDic[originStopID][thisMiddleStopID]["busTimeRV"] = False
            accessDic[originStopID][thisMiddleStopID]["lastTripIDRV"] = False
            return False
        for timeGen, eachArc in arcsList.items():
            if timeGen >= lastTime:  # arrival time is earlier than the generating time in the thisMiddleStopID
                accessDic[originStopID][thisMiddleStopID]["timeRV"] = eachArc["time_rec"] - lastTime
                accessDic[originStopID][thisMiddleStopID]["waitTimeRV"] = timeGen - lastTime
                accessDic[originStopID][thisMiddleStopID]["busTimeRV"] = eachArc["bus_time"]
                accessDic[originStopID][thisMiddleStopID]["lastTripIDRV"] = eachArc["trip_id"]
                break
    
    # if accessDic[originStopID][thisMiddleStopID]["timeRV"] > 2000:
    #     print(thisTripTypeSC, originStopID, accessDic[originStopID][thisMiddleStopID]["timeRV"], accessDic[originStopID][thisMiddleStopID]["timeSC"])
    #     for timeGen, eachArc in arcsList.items():
    #         print(lastTime, timeGen, eachArc["time_gen_S"])
    accessDic[originStopID][thisMiddleStopID]["revisitTag"] = True
    return accessDic[originStopID][thisMiddleStopID]["timeRV"]


def revisitSolver():
    db_access = client.cota_access_rel
    startDate = date(2018, 2, 1)
    # startDate = date(2019, 7, 1)
    endDate = date(2020, 7, 1)
    walkingDistanceLimit = 700
    timeDeltaLimit = 240 * 60
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
            todayTimestampList.append(todaySeconds + i * 60*60)

        for eachTimestamp in todayTimestampList:
            print("-----", todayDate, "-----",
                  int(eachTimestamp), "----- Start")
            col_stops = db_GTFS[str(GTFSTimestamp) + "_stops"]
            col_stop_times = db_GTFS[str(GTFSTimestamp) + "_stop_times"]
            col_real_times = db_real_time["R" + todayDate]
            rl_stop_times_rt = list(col_real_times.find(
                {"time": {"$gt": eachTimestamp, "$lt": eachTimestamp + timeDeltaLimit}}))  # Real-time

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

                    timeGen = eachTrip[index]["time"] # RT time
                    timeRec = eachTrip[index + 1]["time"] # RT time
                    timeGenS = eachTrip[index]["scheduled_time"]
                    timeRecS = eachTrip[index + 1]["scheduled_time"]

                    try:
                        arcsDicRT[generatingStopID][receivingStopID][timeGen]
                    except:
                        arcsDicRT[generatingStopID][receivingStopID][timeGen] = {
                            'time_gen': timeGen, 'time_rec': timeRec, 'bus_time': - timeGen + timeRec, "trip_id": eachTripID, "time_gen_S": timeGenS}

            # Sort the arcsDics with OrderedDic.
            def return_time_gen(e):
                return e[0]
            for startStopID, startStop in arcsDicRT.items():
                for endStopID, endStop in startStop.items():
                    arcsDicRT[startStopID][endStopID] = OrderedDict(
                        sorted(endStop.items(), key=return_time_gen))

            accessDic = {}
            col_access = db_access["test_" +
                                   todayDate + "_" + str(int(eachTimestamp))]
            rl_access = (col_access.find({}))

            for eachOD in tqdm(rl_access):
                originStopID = eachOD["startStopID"]
                destinationStopID = eachOD["receivingStopID"]
                if destinationStopID == None:
                    destinationStopID = originStopID
                
                if eachOD["timeRT"] > 99999 or eachOD["timeSC"] > 99999:
                    continue

                try:
                    accessDic[originStopID]
                except:
                    accessDic[originStopID] = {}

                eachOD["revisitTag"] = False
                eachOD["timeRV"] = None
                eachOD["walkTimeRV"] = None
                eachOD["waitTimeRV"] = None
                eachOD["busTimeRV"] = None
                eachOD["lastTripIDRV"] = None
                accessDic[originStopID][destinationStopID] = eachOD

            for originStopID, ODs in tqdm(accessDic.items()):
                try:
                    accessDic[originStopID][originStopID]
                except:
                    accessDic[originStopID][originStopID] = {
                        "startStopID": originStopID,
                        "receivingStopID": originStopID,
                        "timeRT": 0,
                        "walkTimeRT": 0,
                        "busTimeRT": 0,
                        "waitTimeRT": 0,
                        "generatingStopIDRT": None,
                        "lastTripIDRT": None,
                        "lastTripTypeRT": None,
                        "transferCountRT": 0,
                        "visitTagRT": True,
                        "timeSC": 0,
                        "walkTimeSC": 0,
                        "busTimeSC": 0,
                        "waitTimeSC": 0,
                        "generatingStopIDSC": None,
                        "lastTripIDSC": None,
                        "lastTripTypeSC": None,
                        "transferCountSC": 0,
                        "visitTagSC": True,
                        "stop_lat": None,
                        "stop_lon": None
                    }
                
                try:
                    accessDic[originStopID][originStopID]["stop_lat"]
                except:
                    accessDic[originStopID][originStopID]["stop_lat"] = None
                    accessDic[originStopID][originStopID]["stop_lon"] = None

                # Initialization
                accessDic[originStopID][originStopID]["revisitTag"] = True
                accessDic[originStopID][originStopID]["timeRV"] = 0
                accessDic[originStopID][originStopID]["walkTimeRV"] = 0
                accessDic[originStopID][originStopID]["waitTimeRV"] = 0
                accessDic[originStopID][originStopID]["busTimeRV"] = 0

                for destinationStopID, eachOD in ODs.items():
                    lastStopID = destinationStopID

                    trajectoryList = []  # Store the stopIDs that have not been revisited along the trajectory
                    trajectoryDebugList = []
                    while(lastStopID != originStopID):
                        thisRevisitTag = accessDic[originStopID][lastStopID]["revisitTag"]
                        if thisRevisitTag == True:  # the previous stop has been revisited.
                            break
                        trajectoryList.insert(0, lastStopID)
                        trajectoryDebugList.insert(0, False)
                        # Move to the prior stop
                        lastStopID = accessDic[originStopID][lastStopID]["generatingStopIDSC"]

                    for thisMiddleStopIDIndex in range(len(trajectoryList)):
                        thisMiddleStopID = trajectoryList[thisMiddleStopIDIndex]
                        lastMiddleStopID = accessDic[originStopID][thisMiddleStopID]["generatingStopIDSC"]

                        # Revisit the link between the two sebsequent stops
                        
                        timeRV = revisit(lastMiddleStopID, thisMiddleStopID,
                                originStopID, eachTimestamp, accessDic, arcsDicRT)
                        
                        trajectoryDebugList[thisMiddleStopIDIndex] = timeRV
                        
                        # lastTimeRV = accessDic[originStopID][lastMiddleStopID]["timeRV"]
                        # thisTripTypeSC = accessDic[originStopID][thisMiddleStopID]["lastTripTypeSC"]
                        # print(lastMiddleStopID, lastTimeRV, thisTripTypeSC)
                        
                        

            insertList = []
            for originStopID, ODs in accessDic.items():
                for destinationStopID, eachOD in ODs.items():
                    insertList.append(eachOD)
            client.cota_access_rev[todayDate + "_" +
                                   str(eachTimestamp)].insert_many(insertList)
            print("-----", todayDate, "-----",
                  int(eachTimestamp), "-----", len(insertList))

            break
        break


if __name__ == "__main__":
    revisitSolver()
