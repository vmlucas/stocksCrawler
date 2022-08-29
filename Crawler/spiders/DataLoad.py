import pandas as pd
from pandas import DataFrame 
import numpy
import requests


def loadData(valor,data):    
    
    stocks_list = []
    columns=['date_value','open_value','high_value','low_value','close_value','adj_close_value','volume']
    df = pd.DataFrame([row.split(',') for row in data.split('\n')], 
                   columns=columns)
    df = df.drop([0])   
    df = df.assign(stockname = valor) 
    for index, row in df.iterrows():
        stocks_list.append(format_stock(row))
    
    #post list to the bank api http://127.0.0.1:5000/insert
    headers = {'content-type': 'application/json'}
    url = 'http://127.0.0.1:5000/insert'
    stocks = {"stocks":stocks_list}
    
    r = requests.post(url, json=stocks, headers=headers)
    print(r)
    
    
    
def format_stock(data):   
    return {
        "stockname": str(data['stockname']),
        "date_value" :data['date_value'],
        "open_value" :data['open_value'],
        "high_value" :data['high_value'],
        "low_value" :data['low_value'],
        "close_value" :data['close_value'],
        "adj_close_value" :data['adj_close_value'],
        "volume" :data['volume'],
    }        