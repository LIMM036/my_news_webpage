from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import json
import requests
from requests.exceptions import SSLError
from bs4 import BeautifulSoup
import NaverApi
from Scrapping import Scrapping
from datetime import datetime
import pandas as pd


def daily_sentiment(db):
    collection = db['news']
    d = {
        'positive': 0,
        'negative': 0,
        'neutral': 0
    }
    for data in collection.find():
        sentiment = data['sentiment']
        for key in d.keys():
            if sentiment == key:
                d[key] += 1

    max_value = max(d.values())
    for key, value in d.items():
        if value == max_value:
            d['sentiment'] = key
            break
    d['pubDate'] = collection.find()[0]['pubDate']
    collection = db['sentiments']
    collection.insert_one(d)


def main(args):
    d = args
    client = MongoClient(host=d['host'], port=27017,
                         username=d['username'], password=d['password'])
    db = client[d['db_name']]
    collection = db[d['collection_name']]  # unique key 설정할 collection

    news = NaverApi.News(d['news_client_id'], d['news_client_secret'])
    docs = news.get(d['keywords'])
    docs = news.filtering(docs)
    news.save(collection, docs)

    scrapping = Scrapping()
    scrapping.loop(collection)

    summary = NaverApi.Summary(d['client_id'], d['client_secret'])
    summary.loop(sentence_num=7, collection=collection)

    sentiment = NaverApi.Sentiment(d['client_id'], d['client_secret'])
    sentiment.loop(collection)

    daily_sentiment(db)

    return {'result': 'success'}


# with open('parameters.json', encoding='utf-8') as f:
#     parameters = json.load(f)
# main(parameters)
