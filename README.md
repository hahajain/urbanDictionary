# Analysing Urban Dictionary Data using MongoDB, Elasticsearch and Kafka.

## Queries

Answers to the queries for the POC can be found in the [Queries.md](Queries/Queries.md) file. Analytical questions that we answered:

**Easy**

1. Do critical fields exist in all documents?
2. Are the critical fields NULL or empty?
3. Is the data type of each field correct?
4. What are the most upvoted definitions?
5. What are the most downvoted definitions? 

**Moderate**

6. Who are the users who have written the highest number of meanings?
7. What are the words that have the highest number of meanings?
8. Which words have the overall highest number of upvotes? (Includes definitions written by multiple users)
9. Which words have the overall highest number of downvotes? (Includes definitions written by multiple users)
10. What are the most popular tags? (Popular = have most words) 
11. What are the most successful tags? (Successful = have words with most upvotes)

**Challenging** 

12. Based on number of upvotes and downvotes, how can we rank the meanings of a particular word?
13. How do Urbaners relate words, i.e., which words are synonymous?

## Data Pipeline

Files related to data pipeline can be found in the [Data Pipeline](Data_Pipeline/) folder.

## Elasticsearch

Files related to Elasticsearch can be found in the [Elasticsearch](Elasticsearch/) folder.
