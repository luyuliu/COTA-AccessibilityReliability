
from pymongo import MongoClient
import pandas
from datetime import timedelta, date, datetime

client = MongoClient('localhost', 27017)

db_agg = client.cota_access_football

hour_list = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]

date_list = [date(2018, 9, 1)]

col_list = db_agg.list_collection_names()

for each_date in date_list:
    for hour in hour_list:
        collection_name = "football_" + each_date.strftime("%Y%m%d") + "_" + str(hour)
        rl_agg = list(db_agg[collection_name].find())
        docs = pandas.DataFrame(rl_agg)
        docs.pop("_id")
        filename = r"D:\Luyu\reliability\football\raw_data\csvs"
        docs.to_csv(filename + "\\" + collection_name + ".csv", "," ,index=False)