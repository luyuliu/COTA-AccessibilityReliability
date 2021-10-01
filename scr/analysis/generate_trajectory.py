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

col = client.cota_access_rel["rel_20180725_1532534400"]

rl = col.find({"startStopID": "OLE2825S"})

dic = {}

for e in rl:
    stopID = e["receivingStopID"]
    dic[stopID] = e

# print(dic)
destination = "5THLEONE"

while(True):
    if destination == None:
        break
    try:
        dic[destination]
    except:
        break
    else:
        a = dic[destination]
        origin = a["generatingStopIDSC"]

        print(destination, a["stop_lat"], a["stop_lon"]) 
        destination = origin
    