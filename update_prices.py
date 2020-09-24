# -*- coding: utf-8 -*-
"""
Created on Wed Aug 26 16:23:14 2020
This script will do the next steps:
1 - create necessary directories (if not exists)
2 - connect or create a sqlite3 database
3 - create the table to store prices in the database (if not exists)
4 - update prices, as follows:
    * checks the most recent date of prices in the database
        - if there are no prices in the database, download prices from a formated csv file in github
            to avoid download and process lots of xml files.
        - if there are prices in the database, and the current date is ahead
            of the date of the last prices in the database, then proceed as follows:
                - download and procees the file with prices from bvmfbovespa website;
                - download and process the file with the number of shares of each ticker
                - merge prices and shares
                - upload the data to the database

@author: Diego
"""

import pandas as pd
import sqlite3
import os
import zipfile
import xml.etree.ElementTree as ET
import datetime
import wget

cwd = os.getcwd()
if not os.path.exists("data"):
    os.makedirs("data")
if not os.path.exists("data\\cotahist"):
    os.makedirs("data\\cotahist")
if not os.path.exists("data\\ftp_files"):
    os.makedirs("data\\ftp_files")
if not os.path.exists("data\\temp"):
    os.makedirs("data\\temp")

conn = sqlite3.connect(cwd + '\\data\\finance.db')
db = conn.cursor()
#%% functions
def create_prices():
    query = """
    CREATE TABLE IF NOT EXISTS prices
    (
         date DATE,
         ticker TEXT,
         preult REAL,
         totneg INTEGER,
         quatot INTEGER,
         voltot INTEGER,
         number_shares INTEGER
    )"""
    db.execute(query)
    return

def get_last_database_price_date():
    """
    Returns
    date of the last available price in the database
    """
    query = "SELECT date FROM prices ORDER BY date DESC LIMIT (1)"
    x = pd.read_sql(query, conn)
    if len(x) > 0:
        last_date = datetime.datetime.strptime(x.values[0][0],'%Y-%m-%d %H:%M:%S').date()

    # if the database is new, with no prices, to avoid downloading and processing lots of xml price files,
    # that takes a lot of time to complete, it will take the last available file with prices in github.
    # Even if the github file is not up to date, it will be much easier to download only the missing files
    else:
        df = pd.read_csv("https://raw.githubusercontent.com/dss-Diego/br_stocks/master/data/all_prices_table.csv")
        df.to_sql('prices', conn, if_exists='replace', index=False)
        query = "SELECT date FROM prices ORDER BY date DESC LIMIT (1)"
        x = pd.read_sql(query, conn)
        last_date = datetime.datetime.strptime(x.values[0][0], '%Y-%m-%d %H:%M:%S').date()
    return last_date

def process_file(file):
    for fl in os.listdir(cwd+'\\data\\temp\\'):
        os.remove(cwd+'\\data\\temp\\'+fl)
    file = 'IN'+file+'.zip'
    zipfile.ZipFile(cwd+'\\data\\ftp_files\\'+file).extractall(cwd+'\\data\\temp\\')
    file_name = max(os.listdir(cwd+'\\data\\temp\\'))
    file_version = file_name[8:-40]
    if file_version == '.01':
        ns = '{urn:bvmf.100.01.xsd}'
    if file_version == '.02':
        ns = '{urn:bvmf.100.02.xsd}'
    tree = ET.parse(cwd+'\\data\\temp\\'+file_name)
    root = tree.getroot()
    bizfilehdr = root.find('{urn:bvmf.052.01.xsd}BizFileHdr')
    xchg = bizfilehdr.find('{urn:bvmf.052.01.xsd}Xchg')
    asset = {}
    i = 0
    for bizgrp in xchg.findall('{urn:bvmf.052.01.xsd}BizGrp'):
        for doc in bizgrp.findall(ns + 'Document'):
            for inst in doc.findall(ns + 'Instrm'):
                rptparams  = inst.find(ns + 'RptParams')
                rptdtandtm = rptparams.find(ns + 'RptDtAndTm')
                dt = rptdtandtm.find(ns + 'Dt').text
                fininstrmattrcmon = inst.find(ns + 'FinInstrmAttrCmon')
                asst = fininstrmattrcmon.find(ns + 'Asst').text
                asstdesc = fininstrmattrcmon.find(ns + 'AsstDesc').text
                desc = fininstrmattrcmon.find(ns + 'Desc').text
                instrminf = inst.find(ns + 'InstrmInf')
                eqtyinf = instrminf.find(ns + 'EqtyInf')
                if eqtyinf is not None:
                    tckrsymb = eqtyinf.find(ns + 'TckrSymb').text
                    if tckrsymb[-1:] not in (['F', 'B', 'G', 'L']):
                        spcfctncd = eqtyinf.find(ns + 'SpcfctnCd').text
                        crpnnm = eqtyinf.find(ns + 'CrpnNm').text
                        mktcptlstn = eqtyinf.find(ns + 'MktCptlstn').text
                        asset[i] = { 'date': dt, 'asst': asst, 'assdesc': asstdesc, 'desc': desc,  'spcfctncd': spcfctncd, 'crpnnm': crpnnm, 'ticker': tckrsymb, 'number_shares': int(mktcptlstn)}
                        i += 1       
    df = pd.DataFrame.from_dict(asset, orient = 'index')
    df[['type','b','c']] = df['spcfctncd'].str.split(n=-1, expand = True)
    df = df[~df['type'].isin(['DRN', 'REC', 'BDR', 'CPA', 'DIA', 'DIR'])]
    df = df[df['ticker'].str[-1:] != 'F']
    df = df[df['ticker'].str[-1].str.isnumeric()]
    df = df[['date', 'ticker', 'number_shares']]
    df = df[~df[['date', 'ticker']].duplicated()]

    for fl in os.listdir(cwd+'\\data\\temp\\'):
        os.remove(cwd+'\\data\\temp\\'+fl)
    return df

def get_shares(to_download):
    file = datetime.datetime.strftime(to_download, "%y%m%d")
    wget.download(f'http://www.b3.com.br/pesquisapregao/download?filelist=IN{file}.zip,',cwd+f'\\data\\ftp_files\\{file}.zip')
    zip_c = zipfile.ZipFile(cwd+f'\\data\\ftp_files\\{file}.zip')
    zipfl = zip_c.namelist()
    zip_c.extract(zipfl[0], cwd+'\\data\\ftp_files\\')
    shares = process_file(file)
    print('downloaded file with shares from '+(datetime.datetime.strftime(to_download, '%Y-%m-%d'))) 
    del zip_c
    os.remove(cwd+f'\\data\\ftp_files\\{file}.zip')
    return shares  

def update_prices():
    create_prices()
    last_date = get_last_database_price_date()
    next_date = last_date + datetime.timedelta(days=1)
    if next_date <= datetime.date.today():
        number_of_days = datetime.date.today() - next_date
        for i in range(0, number_of_days.days+1):
            to_download = next_date+datetime.timedelta(days=i)
            weekday = to_download.weekday()
            if weekday<5:
                date = to_download.strftime("%d%m%Y")
                try:
                    file_url = f'http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{date}.ZIP'
                    wget.download(file_url, cwd+'\\data\\cotahist\\')
                    with zipfile.ZipFile(cwd+f'\\data\\cotahist\\COTAHIST_D{date}.ZIP', 'r') as zip_ref:
                        zip_ref.extractall(cwd+'\\data\\temp\\')
                    colspecs = [[0,2],[2,10],[10,12],[12,24],[24,27],[27,39],[39,49],[49,52],[52,56],
                                [56,69],[69,82],[82,95],[95,108],[108,121],[121,134],[134,147],[147,152],
                                [152,170],[170,188],[188,201],[201,202],[202,210],[210,217],[217,230],[230,242],[242,245]]
                    column_names = ['tipreg','date','codbdi','ticker','tpmerc','nomres','especi','prazot',
                                    'modref','preabe','premax','premin','premed','preult','preofc','preofv',
                                    'totneg','quatot','voltot','preexe','indopc','datven','factcot','ptoexe',
                                    'codisi','dismes']
                    prices = pd.read_fwf(cwd+f'\\data\\temp\\COTAHIST_D{date}.TXT', colspecs=colspecs)
                    prices.columns = column_names
                    prices = prices.iloc[:-1,:]
                    prices[['preabe','premax','premin','premed','preult','preofc','preofv','voltot']] = prices[['preabe','premax','premin','premed','preult','preofc','preofv','voltot']] / 100
                    prices[['type', 'b', 'c']] = prices['especi'].str.split(n = -1, expand = True)
                    prices = prices[~prices['type'].isin(['DRN', 'REC', 'BDR', 'CPA', 'DIA', 'DIR'])]
                    prices = prices[prices['ticker'].str[-1:] != 'F']
                    prices = prices[prices['ticker'].str[-1].str.isnumeric()]
                    prices = prices[prices['preexe'] == 0]
                    prices['date'] = pd.to_datetime(prices['date'])
                    prices = prices[['date','ticker','preult','totneg','quatot','voltot']]
                    print("downloaded file with prices of the day " + str(datetime.datetime.strptime(date, '%d%m%Y').date()))
                    shares = get_shares(to_download)
                    df = prices.merge(shares[['ticker', 'number_shares']], how='left', left_on='ticker', right_on='ticker')
                    df.to_sql('prices', conn, if_exists='append', index=False)
                    print("Sucessfully update prices of the day "+str(datetime.datetime.strptime(date, '%d%m%Y').date()))
                except:
                    print("Prices of the day "+str(datetime.datetime.strptime(date, '%d%m%Y').date())+' were not possible to be downloaded. Please try again later.')
    else:
        print("no updates available.")
    return

#%%
      












        
        
        

