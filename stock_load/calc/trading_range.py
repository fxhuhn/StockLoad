import pandas as pd
import numpy as np

def trading_range(price_data : pd.DataFrame) -> pd.DataFrame:
    
    if 'Long_Range' not in price_data.columns:
        price_data['Long_Range'] = pd.Series(dtype='object')  #np.nan
    if 'Short_Range' not in price_data.columns:
        price_data['Short_Range'] = pd.Series(dtype='object')  #np.nan

    #long range
    for i, row in price_data[(price_data[f'Long_Range'].isna())].iterrows():
        offset = price_data.index.get_loc(i)
        range = np.where(price_data[i:].Low.values<row.Low)[0]
        trade = {}
        if len(range) > 0:
            try:
                range = price_data.index[offset+ range[0]]
                trade['Long_Range'] = range
            except:
                range = price_data.index.max()            
        else:
            range = price_data.index.max()
        
        aktiv = np.where(price_data[i:range].High.values>row.High)[0]
            
        if len(aktiv):
            trade['Long_aktiv'] = price_data.index[offset+ aktiv[0]]
            
            tp1 = np.where(price_data[i:range].High.values>(row.High+1*(row.High-row.Low)))[0]
            if len(tp1):
                trade['Long_TP1'] = price_data.index[offset+ tp1[0]]

                tp2 = np.where(price_data[i:range].High.values>(row.High+2*(row.High-row.Low)))[0]
                if len(tp2):
                    trade['Long_TP2'] = price_data.index[offset+ tp2[0]]
                    trade['Long_Range'] = range

                    tp3 = np.where(price_data[i:range].High.values>(row.High+3*(row.High-row.Low)))[0]
                    if len(tp3):
                        trade['Long_TP3'] = price_data.index[offset+ tp3[0]]
        for key, value in trade.items():
            price_data.at[i, key] = value

    #short range
    for i, row in price_data[(price_data[f'Short_Range'].isna())].iterrows():
        offset = price_data.index.get_loc(i)
        range = np.where(price_data[i:].High.values>row.High)[0]
        trade = {}

        if len(range)>0:
            try:
                range = price_data.index[offset + range[0]]
                trade['Short_Range'] = range
            except:
                range = price_data.index.max()    
        else:
            range = price_data.index.max()
        
        aktiv = np.where(price_data[i:range].Low.values<row.Low)[0]
            
        if len(aktiv):
            trade['Short_aktiv'] = price_data.index[offset+ aktiv[0]]
            
            tp1 = np.where(price_data[i:range].Low.values<(row.Low-1*(row.High-row.Low)))[0]
            if len(tp1):
                trade['Short_TP1'] = price_data.index[offset+ tp1[0]]

                tp2 = np.where(price_data[i:range].Low.values<(row.Low-2*(row.High-row.Low)))[0]
                if len(tp2):
                    trade['Short_TP2'] = price_data.index[offset+ tp2[0]]
                    trade['Short_Range'] = range

                    tp3 = np.where(price_data[i:range].Low.values<(row.Low-3*(row.High-row.Low)))[0]
                    if len(tp3):
                        trade['Short_TP3'] = price_data.index[offset+ tp3[0]]

        for key, value in trade.items():
            price_data.at[i, key] = value
      
    #price_data.reset_index(inplace=True)
    return price_data