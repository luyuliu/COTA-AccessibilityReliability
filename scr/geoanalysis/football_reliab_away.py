import sys
import csv
import numpy
from datetime import timedelta, date, datetime
import time
from pymongo import MongoClient
from tqdm import tqdm
client = MongoClient('mongodb://localhost:27017/')


# breakpointList = [date(2018, 1, 1), date(2018, 5, 1), date(2018, 9, 1), date(
#     2019, 1, 1), date(2019, 5, 1), date(2019, 9, 1), date(2020, 1, 1), date(2020, 5, 1)]

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

pm3List = [date(2018,9,8), date(2018,9,22), date(2018,10,6), date(2019, 9, 21)]
pm7List = [date(2019, 10, 5)]
gameHour = 13

# dateList = [date(2018,9,1), date(2018,9,8), date(2018,9,22), date(2018,10,6), date(2018,10,13), date(2018,11,3), date(2018,11,24), date(2019,8,31), date(2019,9,7), date(2019,9,21), date(2019,10,5), date(2019,10,26), date(2019,11,9), date(2019,11,23)]
# dateList = [date(2018,8,25), date(2018,9,15), date(2018,9,29), date(2018,10,20), date(2018,10,27), date(2018,11,10), date(2018,11,17), date(2019,9,14), date(2019,9,28), date(2019,10,12), date(2019,10,19), date(2019,11,2), date(2019,11,9)] # Control group
# dateList = [date(2018, 9, 8)]
daterange = [date(2018,9,1), date(2018,9,8), date(2018,9,22), date(2018,10,6), date(2018,10,13), date(2018,11,3), date(2018,11,24), date(2019,9,7), date(2019,9,21), date(2019,10,5), date(2019,10,26), date(2019,11,9), date(2019,11,23)]    
# daterange = [date(2018, 9, 15), date(2018, 9, 29), date(2018, 10, 20), date(2018, 11, 10), date(2018, 11, 17), date(2018, 12, 3), date(
#     2019, 1, 1), date(2019, 9, 14), date(2019, 9, 28), date(2019, 10, 18), date(2019, 11, 16), date(2019, 11, 30), date(2019, 12, 7)]
# daterange = [date(2018, 10, 27), date(2018, 12, 1), date(2018, 12, 8), date(2019, 10, 12)]
# budgetList = [i for i in range(5, 121, 5)]
# daterange = [date(2018, 10, 6)]
daterange = [date(2018, 12, 3), date(2019, 1, 1), date(2019, 10, 18), date(2019, 11, 16), date(2019, 11, 30), date(2019, 12, 7)]
budgetList = [30]

# leftup = [40.0064, -83.016767]
# rightdown = [39.95232, -82.983164]

# leftup2 = [	40.064282, -83.07086]
# rightdown2 = [39.913961, -82.929287]

dic = {}
for eachDate in tqdm(daterange):
    # if eachDate in pm3List:
    #     continue
    #     gameHour = 15
    # if eachDate in pm7List:
    #     continue
    # else:
    #     gameHour = 12
    todayDate = eachDate.strftime("%Y%m%d")
    for jj in [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]:
        weekday = eachDate.weekday()
        # inList = [0] * len(budgetList)
        SCList = [0] * len(budgetList)
        RVList = [0] * len(budgetList)
        RTList = [0] * len(budgetList)
        SCdiffRVList = [0] * len(budgetList)
        SCdiffRTList = [0] * len(budgetList)
        RTdiffRVList = [0] * len(budgetList)
        # middleList = [0] * len(budgetList)
        # outList = [0] * len(budgetList)
        count = 0
        # inCount = 0
        # outCount = 0
        # middleCount = 0
        todayDate = eachDate.strftime("%Y%m%d")
        startSecond = int(time.mktime(time.strptime(todayDate, "%Y%m%d")) + jj * 3600)
        col = client.cota_access_football_control["football_" + eachDate.strftime("%Y%m%d") + "_" + str(jj)]
        # print(eachDate.strftime("%Y%m%d") + "_" + str(startSecond))
        rl = (col.find())
        # print(len(rl), "REA_" + (i.strftime("%Y%m%d")) + "_" + str(j))
        for od in rl:
            count += 1
            stopID = od["stopID"]
            try:
                dic[stopID]
            except:
                dic[stopID] = {
                    "lat": od["lat"],
                    "lon": od["lon"],
                    "before_value": 0,
                    "before_hour": 0,
                    "after_value": 0,
                    "after_hour": 0
                }
                for jjj in [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]:
                    dic[stopID]["PPA_SC_" + str(jjj)] = 0
                    dic[stopID]["PPA_RV_" + str(jjj)] = 0

            dic[stopID]['PPA_SC_' + str(jj)] += od['PPA_SC_30']
            dic[stopID]['PPA_RV_' + str(jj)] += od['PPA_RV_30']

for stopID, od in dic.items():
    for jjj in [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]:
        dic[stopID]['PPA_SC_' + str(jjj)]
        unreliab = 0
        if od["PPA_RV_" + str(jjj)] != 0:
            unreliab = (od["PPA_SC_" + str(jjj)] - od["PPA_RV_" + str(jjj)])/od["PPA_RV_" + str(jjj)]
        if jjj <= gameHour:
            if unreliab > dic[stopID]["before_value"]:
                dic[stopID]["before_value"] = unreliab
                dic[stopID]["before_hour"] = jjj
        else:
            if unreliab > dic[stopID]["after_value"]:
                dic[stopID]["after_value"] = unreliab
                dic[stopID]["after_hour"] = jjj

client.cota_access_football_agg["GameTime_away"].drop()
for a,b in dic.items():
    client.cota_access_football_agg["GameTime_away"].insert_one(b)
        # print(SCdiffRTList)
        # print(RTdiffRVList)
        # print("  ")

                

            