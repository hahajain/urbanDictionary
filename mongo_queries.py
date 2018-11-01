import pymongo
from pymongo import MongoClient

# Connect to a running mongoDB instance
client = MongoClient()
# Switch to urbanDictionary database in Mongo
db = client.urbanDictionary

# Easy
# 1. Check if critical fields exist in all documents
db.urbanDictionary.count({"_id" : {"$exists" : False}})
db.urbanDictionary.count({"lowercase_word" : {"$exists" : False}})
db.urbanDictionary.count({"tags" : {"$exists" : False}})
db.urbanDictionary.count({"thumbs_up" : {"$exists" : False}})
db.urbanDictionary.count({"thumbs_down" : {"$exists" : False}})
db.urbanDictionary.count({"author" : {"$exists" : false}})

# 2. Check is critical fields exist and they are null

# 3. Check is tags array is empty
db.urbanDictionary.count({"tags" : []}) #734948

# 4. Check data types for important attributes

# 5. Most upvoted meaning
db.urbanDictionary.find().sort({"thumbs_up":-1}).limit(1).pretty()

# 6. Most downvoted meaning - 
db.urbanDictionary.find().sort({"thumbs_down":-1}).limit(1).pretty()

# Moderate
# 7. Which author has given highest number of meanings?
db.urbanDictionary.aggregate([{"$sortByCount" : "$author"}], {"allowDiskUse" : true})

# Anonymous and anonymous
# Change query to:
db.urbanDictionary.aggregate([{"$match" : {"author" : {"$nin" :["Anonymous","anonymous" ]}}},{"$sortByCount":"$author"},{"$limit":1}],{"allowDiskUse":true})

# 8. 

