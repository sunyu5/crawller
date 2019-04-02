# from pymongo import MongoClient

from io import StringIO
import requests
import sys
import pandas as pd
import re
from datetime import datetime
import boto3
import config
# import datetime
from dateutil.parser import parse
import json


# def open_mongodb():
#     """
#     open the mongodb
#     :return: mongodb collection
#     """
#     client = MongoClient('localhost', 27017)
#     db = client['data']
#     collection = db['holders']
#     return collection
#
#
# def save_holder(collection, holder_info):
#     """
#     sava the holder info into MongoDB
#     :param collection: mongodb collection
#     :param holder_info: json info of the web page
#     """
#     result =collection.insert_one(holder_info)


def get_crumb(url, session):
    sys.setrecursionlimit(2000)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5)AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"}
    rep = session.get(url, headers=headers, timeout=2)
    if rep.status_code == requests.codes.ok:
        try:
            crumble_regex = r'CrumbStore":{"crumb":"(.*?)"}'
            match = re.search(crumble_regex, rep.text)
            if match:
                crumb = match.group(1)
            else:
                raise ValueError('Could not get crumb from Yahoo Finance')
            return crumb
        except Exception as e:
            print("url: " + url)
            print(str(e))


def scrape_crypto(session, ticker, start_time, end_time,crumb):
    download_link = "https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={start_time}&period2={end_time}" \
                    "&interval=1d&events=history&crumb={crumb}"
    start_time = int(datetime.strptime(start_time, '%Y-%m-%d').timestamp())
    end_time = int(datetime.strptime(end_time, '%Y-%m-%d').timestamp())
    # end_time = int(datetime.now().timestamp())
    download_url = download_link.format(ticker=ticker, start_time=start_time, end_time=end_time, crumb=crumb)
    print(download_url)
    response = session.get(download_url)
    if response.status_code == 200:
        return pd.read_csv(StringIO(response.text), error_bad_lines=False, header=0)
    else:
        return None


def get_table(table_name):
    db3 = boto3.resource('dynamodb', aws_secret_access_key=config.aws_secret_access_key,
                         aws_access_key_id=config.aws_access_key_id, region_name='us-east-2')
    table = db3.Table(table_name)
    return table


def insert_item_dynamodb(table, info):
    response = table.put_item(Item=info)
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print("insert the item")
    else:
        print("cannot insert the item")


def update_history_item_dynamodb(table, ticker,history_item):
    response = table.update_item(Key={
        'ticker': ticker,
    },UpdateExpression='set #table = list_append(if_not_exists(#table, :empty_list), :history_item)',
        ExpressionAttributeNames={
        '#table': 'table'
    },ExpressionAttributeValues={
        ':history_item': [history_item],
        ':empty_list': []
    })
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print("update the item")
    else:
        print("cannot update the item")


def create_crypto_historical_url(path):
    """
    read csv file and create urls
    :param path: fil path
    :return: list of urls
    """
    # read company symbol
    contents = pd.read_csv(path, header=0)
    ticker_url_pairs = list()
    for index,row in contents.iterrows():
        tickers_url = "https://finance.yahoo.com/quote/" + row[0] + "/history?p=" + row[0]
        ticker_url_pairs.append((tickers_url, row[0], row[1]))
    return ticker_url_pairs


def process_multiple_time_format(time_str):
    dt = parse(time_str)
    datestr = dt.strftime('%Y-%m-%d')
    time = datetime.datetime.strptime(datestr, '%Y-%m-%d')
    return time.timestamp()


def insert_data_mysql(url, data):
    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    #response = requests.post(url, json=data)
    print(response.status_code)
