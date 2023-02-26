import pandas as pd
import yfinance as yf
import numpy as np
import pandas_datareader.data as web
import calc

import yaml
from yaml.loader import SafeLoader

import logging
from logging.handlers import RotatingFileHandler

import multiprocessing
from multiprocessing import Pool


TICKER_2_SYMBOL = {
    'volatility.us'     : '^VIX',
    'sp500-future.us'   : 'ES=F',
    'sp500.us'          : '^GSPC',
    'nasdaq-future.us'  : 'NQ=F',
    'nasdaq.us'         : '^NDX',
    'dowjones.us'       : '^DJI',
    'dowjones-future.us': 'YM=F',
    'mdax.de'           : '^MDAXI',
    #'mdax.de'           : '^MDAX',
    'mdax.us'           : '^MDAXI',
    'sdax.us'           : '^SDAXI',
    'tecdax.us'         : '^TECDAX',
    'dax.de'            : '^GDAXI',
    'fdax.de'           : 'DY.F',
    'mdax-future.us'    : 'DM.F',
    'tecdax-future.us'  : 'DZ.F',
    'stoxx50.de'        : 'FS.F',
    'fb.us'             : 'meta.us',
    'cec1.de'           : 'CEC.DE',
    'a140qa.us'         :  'WEED.TO',
    'eadsf.de'          : 'air.de',
    'wltw.us'           : 'wtw.us',
    'dri.de'            : '1u1.de',
    'qgen.de'           : 'qia.de',
    'rtl.de'            : 'rrtl.de', 
}

def load_stock_data(symbol:str, start:str = '2010-01-01') -> pd.DataFrame:
    #print (symbol, TICKER_2_SYMBOL.get(symbol,''))
    
    df = yf.download(TICKER_2_SYMBOL.get(symbol,symbol).replace('.us',''), start, progress=False)
    
    if len(df)>0:
        #logger.debug(f'Fetched {len(df)} entries for {symbol} on yahoo')
        df = df.rename(columns={'Adj Close': 'Adj_Close'})
        df = df.round({'Open': 2,'High': 2,'Low': 2, 'Close': 2, 'Adj_Close': 2})
        df.index = df.index.strftime('%Y-%m-%d')
    else:
        df = pd.DataFrame()
        logging.warning(f'Stock {symbol} not found on yahoo')
    return df


def export_stock_data(symbol:str, stock_data:pd.DataFrame) -> None:
    
    stock_data.reset_index(inplace=True)
    stock_data.rename(columns={'Date': 'Datum',
                        'Open' : 'Erster',
                        'High': 'Hoch',
                        'Low': 'Tief',
                        'Close': 'Schlusskurs',
                        'Volume': 'Volumen'},  inplace=True) 
    stock_data.to_csv(f'./data/stocks/post_{symbol.replace(".","_")}.csv', sep=';', index=False, date_format='%Y-%m-%d')    


def load_stocks() -> pd.DataFrame:
    stocks = pd.read_csv('./data/stocks.csv', sep=';', header=None)
    
    #rename columns
    stocks.columns = ['Name','Symbol']
    stocks['Provider'] = 'yahoo'
    
    #some cosmetic
    stocks.Symbol = stocks.Symbol.str.strip()
    stocks.Symbol = stocks.Symbol.str.lower()
    
    ##load from stooq
    for symbol in ['fdax.de', 'stoxx50.de', ]:
        stocks.loc[stocks.Symbol==symbol, 'Provider'] = 'stooq'
    
    #delete duplicates
    stocks = stocks.drop_duplicates(subset=['Symbol'], keep='first')

    return stocks


def load_stock_price(symbol:str) -> pd.DataFrame:
    try:
        stock_data = pd.read_csv(f'../data/stocks/raw.{symbol}.csv')
    except (FileNotFoundError, IOError):
        logging.warning(f'File ../data/stocks/raw.{symbol}.csv not found')
        stock_data = pd.DataFrame()
    except Exception as e:
        print(e) 
        logging.error(e, exc_info=True)
        stock_data = pd.DataFrame()
    return stock_data            


def save_stock_price(symbol:str, stock_data:pd.DataFrame) -> None:
    stock_data.to_csv(f'./data/stocks/raw.{symbol}.csv', index=True)
   
    
handlers = [ RotatingFileHandler(filename='stockload.log', 
            mode='w', 
            maxBytes=512000, 
            backupCount=4)
           ]
logging.basicConfig(handlers=handlers, 
                    level=logging.WARNING, 
                    format='%(asctime)s %(levelname)s %(message)s', 
                    datefmt='%m/%d/%Y %H:%M:%S')
     
logger = logging.getLogger('my_logger')

def update_stocks(stock:dict) -> pd.DataFrame:
    stock_data_present = load_stock_price(stock['Symbol'])

    if stock['Provider'] =='yahoo':
        stock_data_new = load_stock_data(stock['Symbol'])
    elif stock['Provider'] =='stooq':
        try:
            stock_data_new = web.DataReader(TICKER_2_SYMBOL.get(stock['Symbol'],stock['Symbol']), "stooq")
        except Exception as e:
            logging.error(f'Stooq Ticker not found {TICKER_2_SYMBOL.get(stock["Symbol"],stock["Symbol"])}')
            logging.error(e, exc_info=True)
            stock_data_new = pd.DataFrame()

    else:
        stock_data_new = pd.DataFrame()

    return (pd.concat([stock_data_present, stock_data_new]))
    

def prepare_stocks(stock_data: pd.DataFrame) -> pd.DataFrame:
    #stock_data = df_clean(stock_data)
    #print(dir(calc))
    stock_data['rsi_7_a'] = calc.rsi(stock_data.Close)
    stock_data['ema_200'] = calc.ema(stock_data.Close, period=200)
    stock_data['ema_50'] = calc.ema(stock_data.Close, period=50)
    stock_data['sma_200'] = calc.sma(stock_data.Close, period=200)
    stock_data['sma_50'] = calc.sma(stock_data.Close, period=50)
    
    stock_data = calc.trading_range(stock_data)
    
    
    stock_data.reset_index(inplace=True)
    stock_data = calc.swings(stock_data)
    stock_data.set_index('Date', inplace=True)
    
    stock_data_week = calc.resample_week(stock_data)
    stock_data_week = calc.trading_range(stock_data_week)
    
    stock_data_week.rename(columns={ 
        'Open' : 'w_open',
        'High': 'w_high',
        'Low': 'w_low',
        'Close': 'w_close',
        'Volume': 'w_volume',
        'Long_Range': 'w_Long_Range',
        'Long_aktiv': 'w_Long_aktiv',
        'Long_TP1' : 'w_Long_TP1',
        'Long_TP2' : 'w_Long_TP2',
        'Long_TP3' : 'w_Long_TP3',
        'Short_Range' : 'w_Short_Range',
        'Short_aktiv' : 'w_Short_aktiv',
        'Short_TP1' : 'w_Short_TP1',
        'Short_TP2' : 'w_Short_TP2',
        'Short_TP3' : 'w_Short_TP3',
    },  inplace=True)
    #stock_data_week.reset_index(inplace=True)
    stock_data = pd.merge(stock_data, stock_data_week, how="left", on="Date")

    stock_data.set_index('Date', inplace=True)


def stockload(stock:dict) -> None :

    #merge existing stockdata with new stock_data
    stock_data = update_stocks(stock)
    
    if len (stock_data) > 0:
        #perform some calculation and agreggation
        stock_data = prepare_stocks(stock_data)
        
        #save local and export in old format
        save_stock_price(stock['Symbol'], stock_data)
        export_stock_data(stock['Symbol'], stock_data)


def main() -> None :
    
    with open('myfile.txt', "w") as f:
        for ticker, symbol in TICKER_2_SYMBOL.items():
            f.write(f'{ticker},{symbol}\n')
    
    with open("test.yaml", "w") as file:
        file.write(yaml.dump(TICKER_2_SYMBOL))
    
    
    with open('test.yaml') as f:
        data = yaml.load(f, Loader=SafeLoader)

    #load stock tickers to be checked
    stocks = load_stocks()
    stock_dict = stocks.to_dict('records')

    #use as many cores as possible
    with Pool(multiprocessing.cpu_count()) as p:
        p.map(stockload, stocks.to_dict('records'))


if __name__ == "__main__":
    main()