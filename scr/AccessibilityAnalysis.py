import BasicSolver
import sys
import os
import time
import multiprocessing
from pymongo import MongoClient, GEOSPHERE
from datetime import timedelta, date, datetime
from math import sin, cos, sqrt, atan2, pi
from tqdm import tqdm
import time as atime
import calendar
client = MongoClient('mongodb://localhost:27017/')


class AccessibilityAnalysis(BasicSolver.BasicSolver):
    def __init__(self, start_date, end_date):
        BasicSolver.BasicSolver.__init__(self)
        self.db_GTFS = client.cota_gtfs
        self.db_real_time = client.cota_real_time
        self.db_lime = client.lime_location
        self.col_lime = self.db_lime.lime_location_all
        self.start_date = start_date
        self.end_date = end_date
        self.db_access = client.cota_access
        # self.sampledStopsList = ['MOREASS', 'HIGCOON', '4TH15TN', 'TREZOLS', 'KARPAUN', 'LIVGRAE', 'GRESHEW', 'MAIOHIW', 'AGL540W', 'WHIJAEE', '3RDCAMW', 'HARZETS', 'MAIBRICE', 'SAI2NDS', '3RDMAIS', 'STYCHAS', 'LOC230N', 'BETDIEW', 'STEMCCS', 'INNWESE', 'HANMAIN', 'HIGINDN', '4THCHIN', 'RIDSOME', 'KARHUYN', 'LIVBURE', 'LONWINE', 'MAICHAW', 'BROHAMIW', 'WHI3RDE', '1STLINW', 'MAINOEW', 'MAIIDLE', '5THCLEE', '3RDTOWS', 'STYGAMS', 'KOE113W', 'TAM464S', 'CAS150S', 'BROOUTE', 'ALUGLENS', 'FRABREN', 'SOU340N', 'HILTINS', 'STRHOVE', 'SAWCOPN', 'HAMWORN', 'DALDUBN', 'MCNCHEN', 'HILBEAS', 'NOROWEN', 'SOUTER2A', 'GENSHAN', 'VACLINIC', 'MORHEATE', 'KOEEDSW1', 'TRAMCKW', 'FAISOUN', 'SAWSAWN', 'CLIHOLE', 'CHAMARN', 'CLE24THN']
        self.sampledStopsList = ["3RDMAIS"]
        self.time_budget = 30 * 60

    def analyzeSingleAccessibilityDifference(self, timestamp):
        stop_difference = {}
        col_scooter_access = self.db_access["abtest_scooter_" + str(timestamp)]
        col_normal_access = self.db_access["abtest_normal_" + str(timestamp)]
        print("--------------------", timestamp, "--------------------")
        for each_start_stop in self.sampledStopsList:
            error_count = 0

            rl_scooter_access = list(col_scooter_access.find({"startStopID": each_start_stop}))
            rl_normal_access = list(col_normal_access.find({"startStopID": each_start_stop}))
            
            rl_scooter_access.sort(key = self.sortFunction)
            rl_normal_access.sort(key = self.sortFunction)

            stop_difference[each_start_stop] = {}
            stop_difference[each_start_stop]["stops"] = {}
            stop_difference[each_start_stop]["scooters"] = {}

            for each_accessible_stop in rl_scooter_access:
                accessible_stop_id = each_accessible_stop["receivingStopID"]
                lastmile_scooter_id = each_accessible_stop["firstScooterID"]
                firstmile_scooter_id = each_accessible_stop["lastScooterID"]
                lastmile_increment = each_accessible_stop["lastScooterIncrement"]
                if lastmile_increment == None:
                    lastmile_increment = 0
                scooter_all_time = each_accessible_stop["time"]

                for each_accessible_stop_normal in rl_normal_access:
                    if accessible_stop_id == each_accessible_stop_normal["receivingStopID"]:
                        normal_all_time = each_accessible_stop_normal["time"]
                        break
                
                total_difference = normal_all_time - scooter_all_time
                firstmile_increment = total_difference - lastmile_increment

                pack = {
                    "accessible_stop_id": accessible_stop_id,
                    "firstmile_increment": firstmile_increment,
                    "lastmile_increment": lastmile_increment,
                    "first_scooter_id": firstmile_scooter_id,
                    "lastmile_scooter_id": lastmile_scooter_id
                }

                try:
                    stop_difference[each_start_stop]["stops"][accessible_stop_id]
                except:
                    stop_difference[each_start_stop]["stops"][accessible_stop_id] = pack
                            
                if firstmile_scooter_id != None:
                    try:
                        stop_difference[each_start_stop]["scooters"][firstmile_scooter_id]
                    except:
                        stop_difference[each_start_stop]["scooters"][firstmile_scooter_id] = {
                            "increment": firstmile_increment,
                            "scooterID": firstmile_scooter_id
                        }
                    else:
                        stop_difference[each_start_stop]["scooters"][firstmile_scooter_id]["increment"] += firstmile_increment

                if lastmile_scooter_id != None:
                    try:
                        stop_difference[each_start_stop]["scooters"][lastmile_scooter_id]
                    except:
                        stop_difference[each_start_stop]["scooters"][lastmile_scooter_id] = {
                            "increment": lastmile_increment,
                            "scooterID": lastmile_scooter_id
                        }
                    else:
                        stop_difference[each_start_stop]["scooters"][lastmile_scooter_id]["increment"] += lastmile_increment

                if (abs(lastmile_increment)>1 or abs(firstmile_increment)>1):
                    if firstmile_scooter_id == None and lastmile_scooter_id == None:
                        print(pack, each_start_stop, accessible_stop_id)
                        error_count += 1
                    
            for each_item in stop_difference[each_start_stop]["scooters"].items():
                print(each_item)
            
            print(error_count, len(rl_scooter_access))
            
            


        # for index, item in stop_difference.items():
        #     print(index, ":", int(item["diff_time"]), int(item["diff_count"]), int(item["diff_time"]))

    def sortFunction(self, value):
        return value["receivingStopID"]

    def outputAnalysis(self):
        daterange = (self.daterange(self.start_date, self.end_date))
        for singleDate in (daterange):
            GTFSTimestamp = self.find_gtfs_time_stamp(singleDate)
            todaySeconds = atime.mktime(singleDate.timetuple())
            sampledStopsList = self.sampledStopsList
            # print("*************** Date: ", singleDate, "; Stops: ", len(sampledStopsList), "***************")
            todayTimestampList = [todaySeconds + 28800, todaySeconds + 46800, todaySeconds + 64800]
            for eachTimestamp in todayTimestampList:
                if eachTimestamp != 1561932000:
                    continue
                self.analyzeSingleAccessibilityDifference(int(eachTimestamp))
                break
        
            # print("***************************************************************************")
            # break
        


if __name__ == "__main__":
    one_date = "20190707"
    start_date = date(2019, 6, 22)
    end_date = date(2019, 12, 18)
    accessibilityAnalysis = AccessibilityAnalysis(start_date, end_date)
    accessibilityAnalysis.outputAnalysis()

