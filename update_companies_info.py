# -*- coding: utf-8 -*-
"""
@author: Diego
"""
import zipfile
import pandas as pd
from bs4 import BeautifulSoup
import requests
import io
import sqlite3
import os
import datetime

cwd = os.getcwd()

pd.set_option("display.width", 400)
pd.set_option("display.max_columns", 10)
pd.options.mode.chained_assignment = None

cwd = os.getcwd()
db = sqlite3.connect(os.path.join(cwd, "data", "finance.db"))
cur = db.cursor()

#%% functions
def create_tables():
    db.execute(
        """CREATE TABLE IF NOT EXISTS files(
                    file_name CHARACTER VARYING (19),
                    last_modified TIMESTAMP
                    )"""
    )
    for fs in ["bpa", "bpp"]:
        db.execute(
            f"""CREATE TABLE IF NOT EXISTS {fs}
                       (cnpj CHARACTER VARYING(18), 
                        dt_refer SMALLINT, 
                        grupo_dfp CHARACTER VARYING(11), 
                        dt_fim_exerc DATE, 
                        cd_conta CHARACTER VARYING(15), 
                        ds_conta CHARACTER VARYING(100), 
                        vl_conta BIGINT, 
                        itr_dfp CHARACTER VARYING(3))"""
        )
    for fs in ["dre", "dva", "dfc"]:
        db.execute(
            f"""CREATE TABLE IF NOT EXISTS {fs}
                       (cnpj CHARACTER VARYING(18), 
                        dt_refer SMALLINT, 
                        grupo_dfp CHARACTER VARYING(11), 
                        dt_ini_exerc DATE, 
                        dt_fim_exerc DATE, 
                        cd_conta CHARACTER VARYING(15), 
                        ds_conta CHARACTER VARYING(100), 
                        vl_conta BIGINT, 
                        itr_dfp CHARACTER VARYING(3), 
                        fiscal_quarter SMALLINT
                        )"""
        )
    db.execute(
        """CREATE TABLE IF NOT EXISTS dmpl
                   (cnpj CHARACTER VARYING(18), 
                    dt_refer INTEGER, 
                    grupo_dfp CHARACTER VARYING(11), 
                    dt_ini_exerc DATE, 
                    dt_fim_exerc DATE, 
                    cd_conta CHARACTER VARYING(15), 
                    ds_conta CHARACTER VARYING(100), 
                    coluna_df CHARACTER VARYING(100), 
                    vl_conta BIGINT, 
                    itr_dfp CHARACTER VARYING(3), 
                    fiscal_quarter SMALLINT)"""
    )
    db.execute(
        """CREATE TABLE IF NOT EXISTS tickers(
                    ticker CHARACTER VARYING(7), 
                    cnpj CHARACTER VARYING(18), 
                    type CHARACTER VARYING(7), 
                    sector CHARACTER VARYING(50), 
                    subsector CHARACTER VARYING(50), 
                    segment CHARACTER VARYING(50), 
                    denom_social CHARACTER VARYING(100), 
                    denom_comerc CHARACTER VARYING(100), 
                    PRIMARY KEY (ticker)
                    )"""
    )
    return

def files_to_update():
    # create dataframe with the files already processed
    db_files = pd.read_sql("SELECT * FROM files", db)

    # create a dataframe with the files that area available in the urls
    urls = [
        "http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/",
        "http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/",
    ]
    files = {}
    i = 0
    for url in urls:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "lxml")
        table = soup.find("table")
        tr = table.find_all("tr")
        for t in tr:
            if (t.text[3:17] == "_cia_aberta_20") and (t.text[19:23] == ".zip"):
                file_name = t.text[0:19]
                last_modified = pd.to_datetime(t.text[23:39])
                files[i] = {"file_name": file_name, "url_date": last_modified}
                i += 1
    available_files = pd.DataFrame.from_dict(files, orient="index")

    # merge both dataframes to check the ones that are new or updated
    new_files = available_files.merge(
        db_files, how="left", right_on="file_name", left_on="file_name"
    )
    new_files['last_modified'] = pd.to_datetime(new_files['last_modified'])
    new_files = new_files.fillna(pd.to_datetime("1900-01-01 00:00:00"))
    new_files = new_files[new_files["url_date"] > new_files["last_modified"]]
    return new_files[['file_name', 'url_date']]

def get_new_file(file):
    type_ = file[0:3]
    file_url = f"http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{type_}/DADOS/{file}.zip"
    response = requests.get(file_url)
    zip_file =  zipfile.ZipFile(io.BytesIO(response.content))

    files_dict = {} # dict with all csv files
    for i in range(len(zip_file.namelist())):
        files_dict[zip_file.namelist()[i]] = zip_file.read(zip_file.namelist()[i])

    return files_dict

def load_fs(files_dict):

    def process_df(df):
        if len(df) > 0: # so times there are empty files

            # cleaning and formating the dataframe
            df.columns = df.columns.str.lower()
            df = df.rename(columns={"cnpj_cia": "cnpj"})

            df = df[df['cnpj'].isin(list(active))]

            df = df[df["ordem_exerc"] == "ÚLTIMO"]
            grupo = df["grupo_dfp"].str.split(" ", expand=True)[1].unique()[0]

            df["dt_refer"] = dt_refer
            df["grupo_dfp"] = grupo
            df["itr_dfp"] = itr_dfp

            # some statements have both, quarter and ytd values. the next line will keep only the ytd.
            df = df.drop_duplicates(
                subset=["cnpj", "dt_fim_exerc", "cd_conta"], keep="first"
            )

            # Start process the dataframe according to the type of finanacial statement:
            if fs in ["bpa", "bpp"]:
                df = df[[
                        "cnpj",
                        "dt_refer",
                        "grupo_dfp",
                        "dt_fim_exerc",
                        "cd_conta",
                        "ds_conta",
                        "vl_conta",
                        "itr_dfp",
                        "escala_moeda",
                    ]]
            if fs in ["dre", "dva", "dfc", "dmpl"]:
                df["dt_ini_exerc"] = pd.to_datetime(df["dt_ini_exerc"])
                df["fiscal_quarter"] = (
                    (
                        (df["dt_fim_exerc"].dt.year - df["dt_ini_exerc"].dt.year)
                        * 12
                        + (
                            df["dt_fim_exerc"].dt.month
                            - df["dt_ini_exerc"].dt.month
                        )
                    )
                    + 1
                ) / 3
                if fs == "dmpl":
                    df = df[[
                            "cnpj",
                            "dt_refer",
                            "grupo_dfp",
                            "dt_ini_exerc",
                            "dt_fim_exerc",
                            "cd_conta",
                            "ds_conta",
                            "coluna_df",
                            "vl_conta",
                            "itr_dfp",
                            "fiscal_quarter",
                            "escala_moeda",
                        ]]
                else:
                    df = df[[
                            "cnpj",
                            "dt_refer",
                            "grupo_dfp",
                            "dt_ini_exerc",
                            "dt_fim_exerc",
                            "cd_conta",
                            "ds_conta",
                            "vl_conta",
                            "itr_dfp",
                            "fiscal_quarter",
                            "escala_moeda",
                        ]]
            df["vl_conta"][df["escala_moeda"] == "UNIDADE"] = df["vl_conta"] / 1000
            df.drop(columns=["escala_moeda"], inplace=True)

            if 'dt_ini_exerc' in df.columns:
                df['dt_ini_exerc'] = df['dt_ini_exerc'].dt.date
            df['dt_fim_exerc'] = df['dt_fim_exerc'].dt.date

            db.execute(
                f"""DELETE FROM {fs} 
                           WHERE dt_refer = {dt_refer} AND 
                                itr_dfp = '{itr_dfp}' AND 
                                grupo_dfp = '{grupo}' """
            )
            df.to_sql(f"{fs}", db, if_exists="append", index=False)
        return


    active = pd.read_sql("SELECT DISTINCT cnpj FROM tickers", db)['cnpj']

    # Delete useless files like itr_cia_aberta_2019.csv
    for key in files_dict.keys():
        if len(key) == 23:
            key_to_delete = key
    del files_dict[key_to_delete]

    # dt_refer: used as a reference to update the database
    dt_refer = list(files_dict)[0][-8:-4]
    # itr or dfp: used to update the database
    itr_dfp = list(files_dict)[0][0:3]

    # deal with cash flows
    # consolidated
    cf_md_con_file = itr_dfp+'_cia_aberta_DFC_MD_con_'+dt_refer+'.csv'
    cf_mi_con_file = itr_dfp+'_cia_aberta_DFC_MI_con_'+dt_refer+'.csv'
    # Individual
    cf_md_ind_file = itr_dfp + '_cia_aberta_DFC_MD_ind_' + dt_refer + '.csv'
    cf_mi_ind_file = itr_dfp + '_cia_aberta_DFC_MI_ind_' + dt_refer + '.csv'
    df_cf_md_con = pd.read_csv(io.BytesIO(files_dict[cf_md_con_file]), sep=';', header=0, encoding='latin-1',
                               parse_dates=['DT_FIM_EXERC'])
    df_cf_mi_con = pd.read_csv(io.BytesIO(files_dict[cf_mi_con_file]), sep=';', header=0, encoding='latin-1',
                               parse_dates=['DT_FIM_EXERC'])
    df_cf_md_ind = pd.read_csv(io.BytesIO(files_dict[cf_md_ind_file]), sep=';', header=0, encoding='latin-1',
                               parse_dates=['DT_FIM_EXERC'])
    df_cf_mi_ind = pd.read_csv(io.BytesIO(files_dict[cf_mi_ind_file]), sep=';', header=0, encoding='latin-1',
                               parse_dates=['DT_FIM_EXERC'])
    df_cf_ind = pd.concat([df_cf_mi_ind, df_cf_md_ind])
    df_cf_con = pd.concat([df_cf_mi_con, df_cf_md_con])
    fs = 'dfc'
    process_df(df_cf_con)
    process_df(df_cf_ind)
    del files_dict[cf_md_con_file]
    del files_dict[cf_mi_con_file]
    del files_dict[cf_md_ind_file]
    del files_dict[cf_mi_ind_file]

    for file in files_dict.keys():
        fs = file[15:-13].lower() # fs -> financial statement
        df = pd.read_csv(io.BytesIO(files_dict[file]), sep=";", header=0, encoding="latin-1", parse_dates=['DT_FIM_EXERC'])
        process_df(df)
    return

def update_db(log=True):

    if log:
        now = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        log_name = f'c_log_{now}.txt'
        with open(os.path.join('data', 'logs', log_name), 'w') as log_file:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log_file.write(f'{now} - Starting\n')

    # create database tables
    create_tables()

    # Update tickers registers with data from github
    tickers = pd.read_csv('https://raw.githubusercontent.com/dss-Diego/br_stocks/master/data/tickers.csv')
    db.execute('DELETE FROM tickers')
    db.commit()
    tickers.to_sql('tickers', db, if_exists='append', index=False)

    new_files = files_to_update()

    if len(new_files) == 0:
        print("All company files are up to date.")
        if log:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(os.path.join('data', 'logs', log_name), 'a') as log_file:
                log_file.write(f'{now} - All company files are up to date.\n')

    for idx, row in new_files.iterrows():
        # 1 - download the zip file

        files_dict = get_new_file(row['file_name'])

        # for each new or updated zip file:
        # 1 - download the zip file and extract all files within
        # 2 - update database with the zip file content
        # 3 - update database with the new file information

        db.execute(
            f"""DELETE FROM files
                       WHERE file_name = '{row['file_name']}'"""
        )
        # 2 - update database with the zip file content
        load_fs(files_dict)
        # 3 - update database with the new file information
        db.execute(
            f"""INSERT INTO files 
                       VALUES ('{row['file_name']}', '{row['url_date']}')"""
        )
        print(f"{row['file_name']} downloaded successfully.")
        if log:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(os.path.join('data', 'logs', log_name), 'a') as log_file:
                log_file.write(f"{now} - {row['file_name']} downloaded successfully.\n")
        db.commit()

    return

#%%

# update_db()


