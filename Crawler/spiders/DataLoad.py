import pandas as pd
from pandas import DataFrame 
import json
import psycopg2
from sqlalchemy import create_engine
import numpy
from psycopg2.extensions import register_adapter, AsIs
import shutil
import os.path

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)

#connect to the PostgreSQL on localhost
def getEngine():
    engine = create_engine('postgresql+psycopg2://postgres:vsvLL430@localhost/postgres')
    return engine   



def loadData(valor,data):    
    engine = getEngine()
    conn = engine.connect()
    trans = conn.begin()

    engine.execute('CREATE TABLE IF NOT EXISTS \
                   bank.stocks(id SERIAL PRIMARY KEY, stockname text, date_value text, open_value float8, high_value float8, low_value float8, close_value float8, adj_close_value float8, volume float8)')

    columns=['date_value','open_value','high_value','low_value','close_value','adj_close_value','volume']
    df = pd.DataFrame([row.split(',') for row in data.split('\n')], 
                   columns=columns)
    df = df.drop([0])   
    df = df.assign(stockname = valor) 
    df.to_sql(name='stocks_tmp',con=engine,schema='bank', if_exists='replace',index=False)

    try:
        engine.execute('delete from bank.stocks \
                          where stockname in (select distinct stockname from bank.stocks_tmp)') 

        engine.execute('insert into bank.stocks(stockname, date_value, open_value, high_value, low_value, close_value, adj_close_value, volume) \
                          select stockname, date_value, cast(open_value as DOUBLE PRECISION), cast(high_value as DOUBLE PRECISION), cast(low_value as DOUBLE PRECISION), cast(close_value as DOUBLE PRECISION), cast(adj_close_value as DOUBLE PRECISION), cast(volume as DOUBLE PRECISION) from bank.stocks_tmp')  

        trans.commit()
        print('Stocks loaded')
    except:
        trans.rollback()
        raise
    finally:
        trans.close()
        conn.close()
        engine.dispose()