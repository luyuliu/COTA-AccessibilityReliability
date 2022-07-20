import sys
import csv
import numpy
from datetime import timedelta, date, datetime
import time
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')


# breakpointList = [date(2018, 1, 1), date(2018, 5, 1), date(2018, 9, 1), date(
#     2019, 1, 1), date(2019, 5, 1), date(2019, 9, 1), date(2020, 1, 1), date(2020, 5, 1)]

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

# dateList = [date(2018,9,1), date(2018,9,8), date(2018,9,22), date(2018,10,6), date(2018,10,13), date(2018,11,3), date(2018,11,24), date(2019,8,31), date(2019,9,7), date(2019,9,21), date(2019,10,5), date(2019,10,26), date(2019,11,9), date(2019,11,23)]
# dateList = [date(2018,8,25), date(2018,9,15), date(2018,9,29), date(2018,10,20), date(2018,10,27), date(2018,11,10), date(2018,11,17), date(2019,9,14), date(2019,9,28), date(2019,10,12), date(2019,10,19), date(2019,11,2), date(2019,11,9)] # Control group
# dateList = [date(2018, 9, 8)]
daterange = [date(2018,9,1), date(2018,9,8), date(2018,9,22), date(2018,10,6), date(2018,10,13), date(2018,11,3), date(2018,11,24), date(2019,8,31), date(2019,9,7), date(2019,9,21), date(2019,10,5), date(2019,10,26), date(2019,11,9), date(2019,11,23)]    

# daterange = [date(2018, 9, 15), date(2018, 9, 29), date(2018, 10, 20), date(2018, 11, 10), date(2018, 11, 17), date(2018, 12, 3), date(
#     2019, 1, 1), date(2019, 9, 14), date(2019, 9, 28), date(2019, 10, 18), date(2019, 11, 16), date(2019, 11, 30), date(2019, 12, 7)]
# daterange = [date(2018, 10, 27), date(2018, 12, 1), date(2018, 12, 8), date(2019, 10, 12)]
# budgetList = [i for i in range(5, 121, 5)]
budgetList = [30]

# leftup = [40.0064, -83.016767]
# rightdown = [39.95232, -82.983164]

# leftup2 = [	40.064282, -83.07086]
# rightdown2 = [39.913961, -82.929287]

for eachDate in daterange:
    SCL = []
    RVL = []
    JL = []
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
        col = client.cota_access_football["football_" + eachDate.strftime("%Y%m%d") + "_" + str(jj)]
        # print(eachDate.strftime("%Y%m%d") + "_" + str(startSecond))
        rl = (col.find())
        # print(len(rl), "REA_" + (i.strftime("%Y%m%d")) + "_" + str(j))
        for od in rl:
            count += 1
            for ja in range(len(budgetList)):
                j = budgetList[ja]
                SCList[ja] += od["PPA_SC_" + str(j)]
                RVList[ja] += od["PPA_RV_" + str(j)]
                RTList[ja] += od["PPA_RT_" + str(j)]
                
        for j in range(len(budgetList)):
            if RVList[j] == 0:
                SCdiffRVList[j] = 0
            else:
                SCdiffRVList[j] = (SCList[j] - RVList[j])/RVList[j]

        SCL.append(SCList[0])
        RVL.append(RVList[0])
        JL.append(jj)
    print(eachDate, RVL)
        # print(SCdiffRTList)
        # print(RTdiffRVList)
        # print("  ")

                

            