# -*- coding: utf-8 -*-
"""
Created on Fri Aug 21 10:02:22 2020

@author: Diego
"""
import sqlite3
import zipfile
import pandas as pd
import os
import wget
from urllib.request import urlopen
from bs4 import BeautifulSoup

cwd = os.getcwd()
if not os.path.exists('data'):
    os.makedirs('data')
if not os.path.exists('data\\cotahist'):
    os.makedirs('data\\cotahist')
if not os.path.exists('data\\ftp_files'):
    os.makedirs('data\\ftp_files')    
if not os.path.exists('data\\temp'):
    os.makedirs('data\\temp')    
conn = sqlite3.connect(cwd + '\\data\\finance.db')
db = conn.cursor()

#%% functions
def create_tables():
    db.execute("""CREATE TABLE IF NOT EXISTS files
                   (file_name TEXT,
                    last_modified DATE)""")
    for fs in ['bpa', 'bpp']:
        db.execute(f"""CREATE TABLE IF NOT EXISTS {fs}
                       (cnpj TEXT, 
                        dt_refer INTEGER, 
                        grupo_dfp TEXT, 
                        dt_fim_exerc DATE, 
                        cd_conta TEXT, 
                        ds_conta TEXT, 
                        vl_conta INTEGER, 
                        itr_dfp TEXT)""")
    for fs in ['dre', 'dva', 'dfc']:
        db.execute(f"""CREATE TABLE IF NOT EXISTS {fs}
                       (cnpj TEXT, 
                        dt_refer INTEGER, 
                        grupo_dfp TEXT, 
                        dt_ini_exerc DATE, 
                        dt_fim_exerc DATE, 
                        cd_conta TEXT, 
                        ds_conta TEXT, 
                        vl_conta INTEGER, 
                        itr_dfp TEXT, 
                        fiscal_quarter INTEGER)""")
    db.execute("""CREATE TABLE IF NOT EXISTS dmpl
                   (cnpj TEXT, 
                    dt_refer INTEGER, 
                    grupo_dfp TEXT, 
                    dt_ini_exerc DATE, 
                    dt_fim_exerc DATE, 
                    cd_conta TEXT, 
                    ds_conta TEXT, 
                    coluna_df TEXT, 
                    vl_conta INTEGER, 
                    itr_dfp TEXT, 
                    fiscal_quarter INTEGER)""")
    db.execute("""CREATE TABLE IF NOT EXISTS tickers
                   (ticker TEXT, 
                    cnpj TEXT, 
                    type TEXT, 
                    sector TEXT, 
                    subsector TEXT, 
                    segment TEXT, 
                    denom_social TEXT, 
                    denom_comerc TEXT, 
                    PRIMARY KEY (ticker))""")
    return

def update_db():
    # create database tables
    create_tables()
    
    # clean temp directory
    for file in os.listdir(cwd+'\\data\\temp'):
        os.remove(cwd+'\\data\\temp\\'+file)
    
    # create dataframe with the files already processed 
    db_files = pd.read_sql("SELECT * FROM files", conn)
    db_files['last_modified'] = pd.to_datetime(db_files['last_modified'])
    
    # create a dataframe with the files that area available in the urls 
    urls = ['http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/', 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/']
    files = {}
    i = 0
    for url in urls:
        html = urlopen(url)
        soup = BeautifulSoup(html, 'lxml')
        table = soup.find('table')
        tr = table.find_all('tr')
        for t in tr:
            if (t.text[3:17] == '_cia_aberta_20') and (t.text[19:23] == '.zip'):
                file_name = t.text[0:19]
                last_modified = pd.to_datetime(t.text[23:39])
                files[i] = {'file_name': file_name, 'url_date': last_modified}
                i += 1
    available_files = pd.DataFrame.from_dict(files, orient='index')
    
    # merge both dataframes to check the ones that are new or updated
    new_files = available_files.merge(db_files, how='left', right_on='file_name', left_on='file_name')
    new_files = new_files.fillna(pd.to_datetime('1900-01-01'))
    new_files = new_files[new_files['url_date'] > new_files['last_modified']]
    if len(new_files) == 0:
        print('All company files are up to date.')
    
    # for each new or updated zip file:
    # 1 - download the zip file and extract all files within
    # 2 - update database with the zip file content
    # 3 - update database with the new file information
    for idx, file in new_files.iterrows():
        # 1 - download the zip file
        type_ = file['file_name'][0:3]
        file_url = f"http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{type_}/DADOS/{file['file_name']}.zip"
        os.chdir(cwd+'\\data\\temp')
        file_name = wget.download(file_url)
        with zipfile.ZipFile(file_name, 'r') as zip_ref:
            zip_ref.extractall()
        os.remove(file_name)
        db.execute(f"""DELETE FROM files
                       WHERE file_name = '{file['file_name']}'""")
        # 2 - update database with the zip file content
        load_fs()
        # 3 - update database with the new file information
        db.execute(f"""INSERT INTO files 
                       VALUES ('{file['file_name']}', '{file['url_date']}')""")
        print(f"{file['file_name']} downloaded successfully.")
        conn.commit()
    os.chdir(cwd)
    return

def load_fs():
    for file in os.listdir(cwd+'\\data\\temp'):
        if len(file) != 23: 
            fs = file[15:-13].lower()
            if len(fs)==6:
                fs = fs[:3]
            itr_dfp = file[0:3]
            df = pd.read_csv(cwd+'\\data\\temp\\'+file, sep = ';', header = 0, encoding = 'latin-1')
            if len(df) > 0:
                df.columns = df.columns.str.lower()
                df = df.rename(columns={'cnpj_cia': 'cnpj'})
                df = df[df['ordem_exerc'] == 'ÚLTIMO']
                grupo = df['grupo_dfp'].str.split(" ", expand = True)[1].unique()[0]
                dt_refer = int(df['dt_refer'].str[0:4].unique()[0])
                df['dt_refer'] = dt_refer
                df['grupo_dfp'] = grupo
                df['itr_dfp'] = itr_dfp
                df = df.drop_duplicates(subset=['cnpj', 'dt_fim_exerc', 'cd_conta'], keep='first')
                df['dt_fim_exerc'] = pd.to_datetime(df['dt_fim_exerc'])
                if fs in ['bpa', 'bpp']:
                    df = df[['cnpj', 'dt_refer', 'grupo_dfp', 'dt_fim_exerc', 'cd_conta', 'ds_conta', 'vl_conta', 'itr_dfp', 'escala_moeda']]
                if fs in ['dre', 'dva', 'dfc', 'dmpl']:
                    df['dt_ini_exerc'] = pd.to_datetime(df['dt_ini_exerc'])
                    df['fiscal_quarter'] = (((df['dt_fim_exerc'].dt.year - df['dt_ini_exerc'].dt.year) * 12 + (df['dt_fim_exerc'].dt.month - df['dt_ini_exerc'].dt.month)) + 1 ) / 3
                    if fs == 'dmpl':
                        df = df[['cnpj', 'dt_refer', 'grupo_dfp', 'dt_ini_exerc', 'dt_fim_exerc', 'cd_conta', 'ds_conta', 'coluna_df', 'vl_conta', 'itr_dfp', 'fiscal_quarter', 'escala_moeda']]        
                    else:
                        df = df[['cnpj', 'dt_refer', 'grupo_dfp', 'dt_ini_exerc', 'dt_fim_exerc', 'cd_conta', 'ds_conta', 'vl_conta', 'itr_dfp', 'fiscal_quarter', 'escala_moeda']]
                df['vl_conta'][df['escala_moeda'] == 'UNIDADE'] = df['vl_conta']/1000
                df.drop(columns=['escala_moeda'], inplace=True)
                db.execute(f"""DELETE FROM {fs} 
                               WHERE dt_refer = {dt_refer} AND itr_dfp = '{itr_dfp}' AND grupo_dfp = '{grupo}' """)
                df.to_sql(f'{fs}', conn, if_exists='append', index=False)
        os.remove(cwd+'\\data\\temp\\'+file)
    return


#%%
        
update_db()  

conn.close()       


