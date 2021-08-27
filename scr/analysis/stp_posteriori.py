# Aggregate the original posteriori OD into posteriori STP. Need to first run 1) Dijkstra Solver
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
import time as atime
sys.path.append(os.path.dirname(os.path.dirname((os.path.abspath(__file__)))))
import transfer_tools
client = MongoClient('mongodb://localhost:27017/')

if __name__ == "__main__":
    startDate = date(2018, 2, 1)
    endDate = date(2020, 7, 1)
    daterange = (transfer_tools.daterange(startDate, endDate))
    numberOfTimeSamples = 1

    budgetList = [i for i in range(0, 121, 5)]

    for singleDate in (daterange):
        todayDate = singleDate.strftime("%Y%m%d")
        GTFSTimestamp = transfer_tools.find_gtfs_time_stamp(singleDate)
        todaySeconds = atime.mktime(singleDate.timetuple())
        gtfsSeconds = str(transfer_tools.find_gtfs_time_stamp(singleDate))
        db_stops = client.cota_gtfs[gtfsSeconds + "_stops"]

        for i in [8, 12, 18]:
            todayTimestamp = (todaySeconds + i * 60*60/numberOfTimeSamples)
            col_access = client.cota_access_rel[todayDate + "_" + str(int(todayTimestamp))]
            print(todayDate + "_" + str(int(todayTimestamp)))
            rl_access = col_access.find({})
            timeDic = {}
            for record in rl_access:
                if record["visitTagSC"] == False or record["receivingStopID"] == None: # NaN for scheduled.
                    continue
                originStop = record["startStopID"]
                destinationStop = record["receivingStopID"]
                try:
                    timeDic[originStop]
                except:
                    rl_stop = db_stops.find_one({"stop_id": originStop})
                    timeDic[originStop] = {
                        "stopID": originStop,
                        "lat": rl_stop["stop_lat"],
                        "lon": rl_stop["stop_lon"],
                    }
                    for budget in budgetList:
                        timeDic[originStop]["countRT_" + str(budget)] = 0
                        timeDic[originStop]["countSC_" + str(budget)] = 0

                timeRT = record["timeRT"]
                timeSC = record["timeSC"]
                for budget in budgetList:
                    if timeRT < budget * 60: # If travel time between the stops are smaller than the budget, then it's accessible 
                        timeDic[originStop]["countRT_" + str(budget)] += 1
                    if timeSC < budget * 60:
                        timeDic[originStop]["countSC_" + str(budget)] += 1

            insertList = []
            for index, record in timeDic.items():
                insertList.append(record)
            
            client.cota_access_agg["stp_" + todayDate + "_" + str(int(todayTimestamp))].drop()
            client.cota_access_agg["stp_" + todayDate + "_" + str(int(todayTimestamp))].insert_many(insertList)
            print("---------------", todayDate, i, "---------------")
            # print(timeDic)
            

