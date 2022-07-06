from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

db = client.cota_access_football

col = db['20191116_1573916400']

db.things.ensureIndex({'source_references.key' : 1}, {unique : true, dropDups : true})