import pymongo
from pymongo import MongoClient

# Connect to a running mongoDB instance
client = MongoClient()
# Switch to the bigData database in Mongo
db = client.bigData

# Easy
# 1. Check if critical fields exist in all documents
db.urbanDictionary.count({"_id" : {"$exists" : False}})
db.urbanDictionary.count({"lowercase_word" : {"$exists" : False}})
db.urbanDictionary.count({"tags" : {"$exists" : False}})
db.urbanDictionary.count({"thumbs_up" : {"$exists" : False}})
db.urbanDictionary.count({"thumbs_down" : {"$exists" : False}})
db.urbanDictionary.count({"author" : {"$exists" : false}})

# 2. Check is critical fields exist and they are null
db.urbanDictionary.count({"_id" : null}); 0
db.urbanDictionary.count({"lowercase_word" : null}); 0
db.urbanDictionary.count({"tags" : null}); 0
db.urbanDictionary.count({"thumbs_up" : null}); 0
db.urbanDictionary.count({"thumbs_down" : null}); 0
db.urbanDictionary.count({"author" : null})

# 3. What number of records have their 'tags' attribute empty?
db.urbanDictionary.count({"tags" : []}) #734948

# 4. Check data types for important attributes
db.urbanDictionary.count({_id : { $not : {$type : "objectId"}}}); 0
db.urbanDictionary.count({lowercase_word : { $not : {$type : "string"}}}); 0
db.urbanDictionary.count({tags : { $not : {$type : "array"}}}); 0
db.urbanDictionary.count({thumbs_up : { $not : {$type : "int"}}}); 0
db.urbanDictionary.count({thumbs_down : { $not : {$type : "int"}}}); 0
db.urbanDictionary.count({author : { $not : {$type : "string"}}})

# 5. Which is the most upvoted definition?
db.urbanDictionary.find().sort({"thumbs_up":-1}).limit(1).pretty()

# 6. Which is the most downvoted definition?
db.urbanDictionary.find().sort({"thumbs_down":-1}).limit(1).pretty()

# Moderate
# 7. Which author has given highest number of meanings?
db.urbanDictionary.aggregate([{"$sortByCount" : "$author"}], {"allowDiskUse" : true})

# To remove "Anonymous" and "anonymous"
# Change query to:
db.urbanDictionary.aggregate([{"$match" : {"author" : {"$nin" :["Anonymous","anonymous" ]}}},{"$sortByCount":"$author"},{"$limit":1}],{"allowDiskUse":true})

# 8. Which tags are most widely used?
db.urbanDictionary.aggregate([{"$unwind" : "$tags"} , {"$sortByCount" : "$tags"}], {"allowDiskUse" : true})

# 9. Which word has the highest number of definitions?
db.urbanDictionary.aggregate([{"$sortByCount" : "$lowercase_word"}, {"$limit":1}], {"allowDiskUse" : true})

# 10. Which word has the overall highest number of upvotes?
query10 = [
    {
      "$group": {
        "_id" : "$lowercase_word",
        "total_upvotes": { $sum: "$thumbs_up" },
      }
    },
    {"$sort": {total_upvotes:-1}},
    {"$limit": 10}
  ]
db.urbanDictionary.aggregate(query10, allowDiskUse = True)

# 11. Which word has the overall highest number of downvotes?
query11 = [
    {
      "$group": {
        "_id" : "$lowercase_word",
        "total_downvotes": { $sum: "$thumbs_down" },
      }
    },
    {"$sort": {total_upvotes:-1}},
    {"$limit": 10}
  ]
db.urbanDictionary.aggregate(query11, allowDiskUse = True)

# 12. What are the most popular tags? (Popular = have most words)
query12 = [
    {"$unwind" : "$tags"},
    {"$sortByCount" : "$tags"}
  ]
db.urbanDictionary.aggregate(query12, allowDiskUse = True)

# 13. What are the most successful tags? (Successful = have words with most upvotes)
query13 = [
    {"$unwind":"$tags"},
    {
      "$group": {
        "_id" : "$tags",
        "total_upvotes": { $sum: "$thumbs_up" }
      }
    },
    {"$sort": {"total_upvotes":-1}},
    {"$limit": 10}
  ]
db.urbanDictionary.aggregate(query13, allowDiskUse = True)

# Challenging
# 14. Based on number of upvotes and downvotes, how can we rank the meanings of a particular word?

# Word = "love"
db.urbanDictionary.find({"lowercase_word":"love"}, {"definition":1, "ratio":1, "_id":0}).sort({"ratio":-1}).pretty().limit(5)
