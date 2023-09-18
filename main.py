from parsing_5 import urls_prepare, parsing_tv
#from data_cleared import data_clear, data_to_numeric
import csv
from pymongo import MongoClient
import requests
if __name__ =='__main__':
    # NASDAQ
    
    nasdaq_tickers = []
    with open('nasdaq_snp.csv', 'r', newline='') as f:
        csv_read = csv.reader(f)
        fields = next(csv_read)
        for item in csv_read:
            for item2 in item:
                nasdaq_tickers.append(item2)

    url_list_nas = urls_prepare(nasdaq_tickers, exchange='NASDAQ')
    url_list_nasdaq_true_on_tv = []
    for url in url_list_nas:
        url_r = requests.get(url)
        if url_r.ok:
            url_list_nasdaq_true_on_tv.append(url)

    for i, url in enumerate(url_list_nasdaq_true_on_tv):
        print(i+1)
        ticker_dict = parsing_tv(url)
        client = MongoClient('localhost', 27017)
        trading = client.trading.temp3
        trading.insert_one(ticker_dict)

    #NYSE
    nyse_tickers = []
    with open('nyse_snp.csv', 'r', newline='') as f:
        csv_read = csv.reader(f)
        for item in csv_read:
            for item2 in item:
                nyse_tickers.append(item2)

    url_list_ny = urls_prepare(nyse_tickers, exchange='NYSE')

    url_list_ny_true_on_tv = []
    for url in url_list_ny:
        url_r = requests.get(url)
        if url_r.ok:
            url_list_ny_true_on_tv.append(url)

    print(url_list_ny_true_on_tv)


    for i, url in enumerate(url_list_ny_true_on_tv):
        print(i+1)
        ticker_dict = parsing_tv(url)
        client = MongoClient('localhost', 27017)
        trading = client.trading.temp3
        trading.insert_one(ticker_dict)












