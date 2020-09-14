# -*- coding: utf-8 -*-
"""
Created on Mon Aug 24 15:19:36 2020

@author: Diego
"""

import sqlite3
import pandas as pd
import os

cwd = os.getcwd()
conn = sqlite3.connect(cwd+'\\data\\finance.db')
db = conn.cursor()

#%%
def update_tickers():
    
    query = "SELECT date FROM prices ORDER BY date DESC LIMIT (1)"
    last_date = pd.read_sql(query, conn)
    last_date = last_date.values[0][0]
        
    tickers = pd.read_csv(cwd+'\\tickers.csv')
    tickers.to_sql('tickers', conn, if_exists='replace', index=False)
    last_tickers = pd.read_sql(f"SELECT DISTINCT ticker FROM prices WHERE date = '{last_date}'", conn)
    last_tickers = last_tickers['ticker'].to_list()
    
    # tem no prices mas nao no tickers:
    missing_tickers = []
    for tckr in last_tickers:
        if tckr not in tickers['ticker'].to_list():
            if (len(tckr) == 5 ) & (tckr[-1:] == '3'):
                missing_tickers.append(tckr)
    
    if len(missing_tickers) > 0:
        print('tickers to update:')            
        for tckr in missing_tickers:
            print(tckr)
    else:
        print("All tickers are up to date.")
    
    # Update example:
    ticker = 'SOMA3'.upper() 
    cnpj = '10.285.590/0001-08'
    type_ = 'oN'.upper()
    sector = 'consumer cyclical'.title()
    subsector = 'retail'.title()
    segment = 'apparel, fabric and footwear'.title()
    denom_social = 'GRUPO DE MODA SOMA S.A.'.upper()
    denom_comerc = 'RBX RIO COMÉRCIO DE ROUPAS S.A.'.upper()
    
    if sector.find(" And ") != -1:
        sector = sector.replace(' And ', ' and ')
    if subsector.find(" And ") != -1:
        subsector = subsector.replace(' And ', ' and ')
    if segment.find(" And ") != -1:
        segment = segment.replace(' And ', ' and ')
    
    db.execute(f"DELETE FROM tickers WHERE ticker = '{ticker}'")
    db.execute(f"""INSERT INTO tickers
                   VALUES ('{ticker}', '{cnpj}', '{type_}', '{sector}', '{subsector}', '{segment}', '{denom_social}', '{denom_comerc}')""")
    conn.commit()
    
    tickers = pd.read_sql("SELECT * FROM tickers", conn)
    tickers.to_csv(cwd+'\\tickers.csv', index=False)
    return




