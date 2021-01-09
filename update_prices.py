import pandas as pd
import xml.etree.ElementTree as ET
import os
import zipfile
import requests
import io
import sqlite3
from datetime import datetime, timedelta, date

pd.set_option("display.width", 400)
pd.set_option("display.max_columns", 10)
pd.options.mode.chained_assignment = None

cwd = os.getcwd()
if not os.path.exists("data"):
    os.makedirs("data")
if not os.path.exists(os.path.join('data', 'logs')):
    os.makedirs(os.path.join('data', 'logs'))

db = sqlite3.connect(os.path.join(cwd, "data", "finance.db"))
cur = db.cursor()

def create_tables():
    query = """
        CREATE TABLE IF NOT EXISTS prices(
            date DATE,
            ticker CHARACTER VARYING(12),
            preult REAL,
            totneg INTEGER,
            quatot INTEGER,
            voltot INTEGER,
            number_shares BIGINT
        )"""
    db.execute(query)
    db.execute("CREATE INDEX idx_prices_ticker ON prices(ticker);")

def next_price_dates():
    """
    returns a range of dates that are used to download prices.
    It will returns dates that:
        * before or equal today;
        * are not brazillian holidays;
        * are not weekends.
    Returns: pd.Series
    """
    query = "SELECT date FROM prices ORDER BY date DESC LIMIT(1)"
    last_db_date = db.execute(query).fetchone()
    if last_db_date is not None:
        last_db_date = datetime.strptime(db.execute(query).fetchone()[0], '%Y-%m-%d').date()
    else:
        # if the database is new, with no prices, to avoid downloading and processing lots of xml price files,
        # that takes a lot of time to complete, it will take the last available file with prices in github.
        # Even if the github file is not up to date, it will be much easier to download only the missing files
        prices = pd.read_csv(
            "https://raw.githubusercontent.com/dss-Diego/br_stocks/master/data/all_prices_table.csv",
            parse_dates=['date']
        )
        prices['date'] = prices['date'].dt.date
        prices.to_sql('prices', db, if_exists='append', index=False)
        query = 'SELECT date FROM prices ORDER BY date DESC LIMIT (1)'
        last_db_date = datetime.strptime(db.execute(query).fetchone()[0], '%Y-%m-%d').date()

    # next_date -> last date in the database + 1 day
    next_date = last_db_date + timedelta(days=1)
    brazilian_holidays = pd.read_excel('https://www.anbima.com.br/feriados/arqs/feriados_nacionais.xls')[:-9]['Data']
    current_year = date.today().year
    brazilian_holidays = brazilian_holidays.append(pd.Series(datetime(current_year, 12, 24))) # current christmas eve
    brazilian_holidays = brazilian_holidays.append(pd.Series(datetime(current_year-1, 12, 24)))  # previous christmas eve
    brazilian_holidays = brazilian_holidays.append(pd.Series(datetime(current_year, 12, 31)))  # current new year's eve
    brazilian_holidays = brazilian_holidays.append(pd.Series(datetime(current_year-1, 12, 31)))  # previous new year's eve

    data_range = pd.date_range(start=next_date, end=date.today(), freq='B').to_series()

    # remove holidays
    data_range = data_range[~data_range.isin(brazilian_holidays)]

    return data_range

def get_prices_file(date, log_name=False):
    """
    Download the zip file with prices, and returns the content as bytes
    Args:
        date: Timestamp
    Returns:
        bytes
    """

    date = date.strftime('%d%m%Y')
    file_url = f'http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{date}.ZIP'
    response = requests.get(file_url)

    if response.status_code == 404:
        print('Prices from '+str(date)+' are not available. Please try again later.')
        if log_name != False:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(os.path.join('data', 'logs', log_name), 'a') as log_file:
                log_file.write(f'{now} - Prices from '+str(date)+' are not available. Please try again later.\n')
        return None
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    bytes_data = zip_file.read(zip_file.namelist()[0])

    return bytes_data

def process_prices_file(bytes_data):
    """
    takes bytes with prices (from get_prices_file function)
    process the data to return the prices in a dataframe
    Args:
        bytes_data: bytes (from get_prices_file) function
    Returns:
        dataframe
    """

    # define colspecs and column names and put data into a dataframe
    colspecs = [[0, 2], [2, 10], [10, 12], [12, 24], [24, 27], [27, 39], [39, 49], [49, 52], [52, 56],
                [56, 69], [69, 82], [82, 95], [95, 108], [108, 121], [121, 134], [134, 147], [147, 152],
                [152, 170], [170, 188], [188, 201], [201, 202], [202, 210], [210, 217], [217, 230], [230, 242],
                [242, 245]]
    column_names = ['tipreg', 'date', 'codbdi', 'ticker', 'tpmerc', 'nomres', 'especi', 'prazot',
                    'modref', 'preabe', 'premax', 'premin', 'premed', 'preult', 'preofc', 'preofv',
                    'totneg', 'quatot', 'voltot', 'preexe', 'indopc', 'datven', 'factcot', 'ptoexe',
                    'codisi', 'dismes']
    prices = pd.read_fwf(io.BytesIO(bytes_data), colspecs=colspecs)
    prices.columns = column_names

    # clean the data, removing last line, converting formats and dropping unnecessary rows
    prices = prices.iloc[:-1]
    cols_to_convert = ['codbdi', 'tpmerc', 'totneg', 'quatot', 'voltot']
    prices[cols_to_convert] = prices[cols_to_convert].astype(int)
    prices = prices.drop(columns=['tipreg', 'nomres', 'prazot', 'modref', 'preabe', 'premax', 'premin',
                                  'premed', 'preofc', 'preofv', 'indopc', 'factcot', 'ptoexe', 'codisi', 'dismes', 'datven', 'preexe'])
    prices[['preult', 'voltot']] = prices[['preult', 'voltot']] / 100

    # drop data according to tpmerc
    """
    030 - mercado a termo
    020 - mercado fracionario
    017 - leilao
    70 - opcoes de compra
    80 - opcoes de venda
    12 - exercicio de opcoes de compra
    13 - exercicio de opcoes de venda
    """
    prices = prices[~prices['tpmerc'].isin([30, 20, 17, 70, 80, 12, 13])]

    # drop data according to codbdi
    """
    10 - direitos e recibos
    14 - cert. invest/tit.div.publica
    22 - bonus privados
    12 - fiis
    """
    prices = prices[~prices['codbdi'].isin([10, 14, 22, 12])]
    prices = prices[~prices['especi'].str[0:3].isin(['DRN', 'DR3'])]
    prices = prices.drop(columns=['codbdi', 'tpmerc', 'especi'])
    prices['date'] = pd.to_datetime(prices['date']).dt.date

    return prices

def get_shares_file(date):
    """
    Download the file with the number of shares and returns bytes
    Args:
        date: Timestamp
    Returns: bytes
    """
    file_date = datetime.strftime(date, "%y%m%d")
    file_url = f'http://www.b3.com.br/pesquisapregao/download?filelist=IN{file_date}.zip'
    response = requests.get(file_url)

    # original zipfile. Inside this one, there is another zipfile
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))

    # The zipfile that is inside the original zipfile. Inside this one, is the XML file
    zip_file2 = zipfile.ZipFile(io.BytesIO(zip_file.read(zip_file.namelist()[0])))

    # Inside the zip_file2 there are 2 xml files. We need the latest one.
    xml_bytes_data = zip_file2.read(max(zip_file2.namelist()))

    return xml_bytes_data

def process_xml_bytes_data(xml_bytes_data, ns='{urn:bvmf.100.02.xsd}'):
    """
    Takes the bytes with the xml from the function get_shares_file.
    The xml file has the number of shares, that are not available in the prices files
    Args:
        xml_bytes_data: bytes (from the get_shares_file function)
        ns: str. Indicates the version of the xml file. Options are:
            '{urn:bvmf.100.01.xsd}' --> files until 2016-01-14
            '{urn:bvmf.100.02.xsd}' --> files after 2016-01-14
    Returns: dataframe
    """

    tree = ET.parse(io.BytesIO(xml_bytes_data))
    root = tree.getroot()
    bizfilehdr = root.find('{urn:bvmf.052.01.xsd}BizFileHdr')
    xchg = bizfilehdr.find('{urn:bvmf.052.01.xsd}Xchg')
    assets = {}
    i = 0
    for bizgrp in xchg.findall('{urn:bvmf.052.01.xsd}BizGrp'):
        for doc in bizgrp.findall(ns + 'Document'):
            for inst in doc.findall(ns + 'Instrm'):

                # find rptparams
                rptparams  = inst.find(ns + 'RptParams')

                # inside rptparams finds rptdtandtm
                rptdtandtm = rptparams.find(ns + 'RptDtAndTm')
                # inside rptdtandtm finds dt
                dt = rptdtandtm.find(ns + 'Dt').text

                fininstrmattrcmon = inst.find(ns + 'FinInstrmAttrCmon')
                mkt = int(fininstrmattrcmon.find(ns + 'Mkt').text)

                # instrminf can be equity or option
                instrminf = inst.find(ns + 'InstrmInf')

                eqtyinf = instrminf.find(ns + 'EqtyInf')

                if eqtyinf is not None:
                    sctyctgy = int(eqtyinf.find(ns + 'SctyCtgy').text)
                    tckrsymb = eqtyinf.find(ns + 'TckrSymb').text
                    spcfctncd = eqtyinf.find(ns + 'SpcfctnCd').text
                    crpnnm = eqtyinf.find(ns + 'CrpnNm').text
                    mktcptlstn = int(eqtyinf.find(ns + 'MktCptlstn').text)
                    if eqtyinf.find(ns + 'LastPric') is not None:
                        lastpric = float(eqtyinf.find(ns + 'LastPric').text)
                    else:
                        lastpric = 0.0

                    assets[i] = dict(
                        date=dt,
                        mkt=mkt,
                        sctyctgy=sctyctgy,
                        ticker=tckrsymb,
                        spcfctncd=spcfctncd,
                        crpnnm=crpnnm,
                        number_shares=mktcptlstn,
                        lastpric=lastpric
                    )
                    i += 1

    stocks = pd.DataFrame.from_dict(assets, orient='index')

    # drop data according to mkt code
    """
    030 - mercado a termo
    020 - mercado fracionario
    017 - leilao
    70 - opcoes de compra
    80 - opcoes de venda
    12 - exercicio de opcoes de compra
    13 - exercicio de opcoes de venda
    """
    stocks = stocks[~stocks['mkt'].isin([30, 20, 17, 70, 80, 12, 13])]

    stocks = stocks[stocks['spcfctncd'].str[0:2] != 'CI']

    """
    sctyctgy 
    1 - DRN
    9 - REC
    6 - FIIS
    3 - ETF
    12 - DIR
    23 - BNS
    13 - UNT
    16 - indexes
    21 - ETF
    23 - BNS
    11 - stocks
    """
    stocks = stocks[~stocks['sctyctgy'].isin([9, 1, 23, 12, 23, 6, 3, 21, 16])]
    stocks = stocks[~stocks['spcfctncd'].str[0:3].isin(['DIA', 'CPA'])]

    return stocks

def merge_shares(prices, shares):
    """
    Merge the dataframe with prices with the dataframe with number of shares
    Args:
        prices: dataframe (from the process_prices_file function)
        shares: dataframe (from the process_xml_bytes_data function)
    Returns: dataframe
    """
    shares = shares[~shares[['date', 'ticker']].duplicated()]
    shares['date'] = pd.to_datetime(shares['date'])
    prices['date'] = pd.to_datetime(prices['date'])
    prices_and_shares = prices.merge(shares[['date', 'ticker', 'number_shares']], how='left',
                                     left_on=['date', 'ticker'], right_on=['date', 'ticker'])
    prices_and_shares['date'] = prices_and_shares['date'].dt.date
    return prices_and_shares

def update_prices(log=True):
    """
    Pipeline to update the prices
    """

    if log:
        now = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        log_name = f'p_log_{now}.txt'
        with open(os.path.join('data', 'logs', log_name), 'w') as log_file:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log_file.write(f'{now} - Starting\n')
    else:
        log_name = False

    # create database tables
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name='prices';"
    if cur.execute(query).fetchone() == None:
        create_tables()

    dates = next_price_dates()

    for date in dates:
        txt_file_name = get_prices_file(date)
        if txt_file_name == None:
            break
        prices = process_prices_file(txt_file_name)

        xml_bytes_data = get_shares_file(date)
        shares = process_xml_bytes_data(xml_bytes_data)

        stock_prices = merge_shares(prices, shares)

        stock_prices.to_sql('prices', db, if_exists='append', index=False)

        print('Updated prices from ' + str(stock_prices.iloc[-1]['date'])[0:11])
        if log:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(os.path.join('data', 'logs', log_name), 'a') as log_file:
                log_file.write(f'{now} - Updated prices from ' + str(stock_prices.iloc[-1]['date'])[0:11] + '\n')
    return None

# update_prices()


# cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
# print(cur.fetchall())


# prices = pd.read_sql('select * from prices', db)
# prices.columns
#
