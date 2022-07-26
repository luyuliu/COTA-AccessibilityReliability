import sys
import csv
from tracemalloc import stop
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

startDate = date(2019, 3, 1)
midDate = date(2020, 3, 1)
endDate = date(2021, 3, 1)
daterange = []
for n in range(int((endDate - startDate).days)):
    currentDate = startDate + timedelta(n)
    if currentDate.weekday() == 2:
        daterange.append(currentDate)

skipList = ['4/17/2019', "5/1/2019", "7/3/2019", "10/16/2019", "3/18/2020", "3/25/2020", "5/6/2020", "5/13/2020", "5/20/2020", "9/16/2020", "2/10/2021"]
# leftup = [40.0064, -83.016767]
# rightdown = [39.95232, -82.983164]

# leftup2 = [	40.064282, -83.07086]
# rightdown2 = [39.913961, -82.929287]
dic = {}

for eachDate in tqdm(daterange):
    jj = 8
    budget = 30
    weekday = eachDate.weekday()
    todayDate = eachDate.strftime("%Y%m%d")
    skiptesttodayDate = eachDate.strftime("%m/%d/%Y")
    if skiptesttodayDate in skipList:
        continue
    else:
        print(eachDate)
    startSecond = int(time.mktime(time.strptime(todayDate, "%Y%m%d")) + jj * 3600)
    if eachDate <= midDate:
        col = client.cota_access_daily_agg["REA_" + eachDate.strftime("%Y%m%d") + "_" + str(jj)]
    else:
        col = client.cota_access_covid["stp_" + eachDate.strftime("%Y%m%d") + "_" + str(jj)]
    # print(eachDate.strftime("%Y%m%d") + "_" + str(startSecond))
    rl = (col.find())
    # print(len(rl), "REA_" + (i.strftime("%Y%m%d")) + "_" + str(j))
    for od in rl:
        stopID = od["stopID"]
        try:
            dic[stopID]
        except:
            dic[stopID] = {
                "stopID": stopID,
                "lat": od["lat"],
                "lon": od["lon"],
                "precovid_count": 0,
                "covid_count": 0,
                "precovid_stp_RV": 0,
                "precovid_stp_SC": 0,
                "covid_stp_RV": 0,
                "covid_stp_SC": 0
            }
        if eachDate <= midDate:
            dic[stopID]['precovid_count'] += 1
            dic[stopID]["precovid_stp_SC"] += od["PPA_SC_" + str(budget)]
            dic[stopID]["precovid_stp_RV"] += od["PPA_RV_" + str(budget)]
        else:
            dic[stopID]['covid_count'] += 1
            dic[stopID]["covid_stp_SC"] += od["PPA_SC_" + str(budget)]
            dic[stopID]["covid_stp_RV"] += od["PPA_RV_" + str(budget)]

client.cota_access_covid["covid_agg_stops"].drop()
for a, b in dic.items():
    client.cota_access_covid["covid_agg_stops"].insert_one(b)
                    

            