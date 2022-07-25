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

startDate = date(2019, 1, 1)
endDate = date(2020, 3, 1)
daterange = []
for n in range(int((endDate - startDate).days)):
    currentDate = startDate + timedelta(n)
    if currentDate.weekday() == 2:
        daterange.append(currentDate)
budgetList = [30]

# leftup = [40.0064, -83.016767]
# rightdown = [39.95232, -82.983164]

# leftup2 = [	40.064282, -83.07086]
# rightdown2 = [39.913961, -82.929287]

for eachDate in daterange:
    SCL = []
    RVL = []
    JL = []
    for jj in [8]:
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
        col = client.cota_access_daily_agg["REA_" + eachDate.strftime("%Y%m%d") + "_" + str(jj)]
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
        JL.append(count)
    print(eachDate, RVL[0], SCL[0], JL[0])
        # print(SCdiffRTList)
        # print(RTdiffRVList)
        # print("  ")

                

            