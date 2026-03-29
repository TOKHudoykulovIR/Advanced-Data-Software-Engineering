from elasticsearch import Elasticsearch, helpers
import json

es = Elasticsearch(
    "http://localhost:9200",
    basic_auth=("elastic", "elastic")
)

with open("c:/Users/tokhi/Desktop/ITPU/SEMEST II/Adv Data Eng/Task 5/Data_Description_Rating.json", encoding="utf-8") as f:
    movies = json.load(f)

helpers.bulk(es, [
    {
        "_index": "movies",
        "_id": m["movieId"],
        "_source": m
    }
    for m in movies
])

print("✅ Data indexed successfully!")