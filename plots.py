# -*- coding: utf-8 -*-
"""
Created on Sat Sep 19 17:06:40 2020

@author: Diego
"""
import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
import matplotlib.dates as mdates
import numpy as np

def bs_plot(df, denom_comerc, grupo):

    # drop % change columns
    df = df.iloc[:,::2]
    # drop all zero rows
    df = df.loc[(df!=0).any(axis=1)]
    # leave only third level accounts
    df = df[df.index.get_level_values(0).str.len() == 7]
    # get only assets from balance sheet
    assets = df[df.index.get_level_values(0).str[0]=='1']
    assets = assets.T
    # get only liabilities and equity
    liab_eqty = df[df.index.get_level_values(0).str[0]=='2']
    liab_eqty = liab_eqty.T

    balance_sheet = [assets, liab_eqty]
    title = [' Balance Sheet - Assets - ', ' Balance Sheet - Liabilities and Shareholdes Equity - ']
    for i in range(2):
        fig, ax = plt.subplots(figsize=(16,9), facecolor='Grey')
        bottom = 0
        for j in range(len(balance_sheet[i].columns)):
            ax.bar(x=balance_sheet[i].index.get_level_values(0),
                    height = balance_sheet[i].iloc[:,j],
                    width=20,
                    label = balance_sheet[i].columns[j][0] + ' ' + balance_sheet[i].columns[j][1],
                    bottom = bottom)
            bottom += balance_sheet[i].iloc[:,j]
        ax.legend(loc='upper left')
        ax.set_xlabel('Date')
        labels = balance_sheet[i].index.get_level_values(0).sort_values(ascending=True)
        ax.set_xticklabels(labels=pd.to_datetime(labels).date, rotation=45, ha='right')
        ax.set_xticks(pd.to_datetime(labels).date)
        ax.set_ylabel('Value (R$,000)')
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        ax.set_title(denom_comerc + title[i] + grupo)
        ax.grid()
        plt.tight_layout()
        plt.show()

def is_account_plot(df, acc_number):

    # drop % change columns
    df = df.iloc[:,::2]
    # drop all zero rows
    df = df.loc[(df!=0).any(axis=1)]
    df = df.T
    # keep only the account to plot
    df = df.loc[:,acc_number].reset_index()
    acc_desc = df.columns[2]
    # list with value types (ytd, ttt, quarter)
    value_types = df['value'].unique()
    # dictionay with all dfs to plot
    plots = {}
    # one image for each value type
    for value in value_types:
        df_plot = df[df['value'] == value]
        df_plot['month'] = df_plot['date'].dt.month
        df_plot['year'] = df_plot['date'].dt.year
        df_plot = df_plot.pivot(index='year',
                                columns='month',
                                values=acc_desc)
        df_plot = df_plot.T
        plots.update({value: df_plot})
    i = 0
    if len(value_types) == 1:
        fig, ax = plt.subplots(len(value_types), 1, sharex=True, figsize=(9,9), facecolor='Grey')
        ax_df = plots[value_types[i]]
        for j in range(len(ax_df.columns)):
            ax.plot(ax_df.iloc[:,j], label=ax_df.columns[j], marker='o')
            ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        ax.legend(loc='upper left')
        ax.set_title(value_types[i])
        ax.grid()
    else:
        fig, axs = plt.subplots(len(value_types), 1, sharex=True, figsize=(9,9), facecolor='Grey')
        for ax in axs:
            ax_df = plots[value_types[i]]
            for j in range(len(ax_df.columns)):
                ax.plot(ax_df.iloc[:,j], label=ax_df.columns[j], marker='o')
                ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
            ax.legend(loc='upper left')
            ax.set_title(value_types[i])
            ax.grid()
            i += 1
    fig.suptitle(acc_number + ' ' + acc_desc)
    plt.xlabel('Month')
    fig.text(0.01, 0.5, 'Account Value (R$,000)', va='center', rotation='vertical')
    plt.xticks([3, 6, 9, 12], ['Mar', 'Jun', 'Sep', 'Dec'])
    plt.show()

def bar_plot(df, denom_comerc, grupo, bars):

    value_types = df.columns
    i = 0
    if len(value_types) == 1 :
        fig, ax = plt.subplots(1, len(value_types), figsize=(20,7), facecolor='Grey')
        ax.bar(x=df.index,
               height=df.iloc[:,i],
               label=df.columns[i],
               width = 25)
        ax.legend(loc='upper left')
        ax.grid()
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        ax.set_xticklabels(labels=pd.to_datetime(df.index).date, rotation=45, ha='right')
        ax.set_xticks(pd.to_datetime(df.index).date)
    else:
        fig, axs = plt.subplots(1, len(value_types), figsize=(20, 7), facecolor='Grey')
        for ax in axs:
            ax.bar(x=df.index,
                   height=df.iloc[:,i],
                   label=df.columns[i],
                   width = 25)
            ax.legend(loc='upper left')
            ax.grid()
            ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
            ax.set_xticklabels(labels=pd.to_datetime(df.index).date, rotation=45, ha='right')
            ax.set_xticks(pd.to_datetime(df.index).date)
            i += 1
    fig.suptitle(denom_comerc + bars + grupo)
    plt.tight_layout()
    plt.show()

def line_plot(df, denom_comerc, grupo, line):
    mean = df.iloc[:,0].mean()
    median = df.iloc[:,0].median()
    df = df.ffill()
    fig, ax = plt.subplots(1,1, figsize=(16,9), facecolor = 'Grey')
    ax.plot(df, label = line)
    xmin, xmax = ax.get_xlim()
    ax.plot([xmin, xmax], [median, median], label=('median = ' + f"{median:.1f}" ))
    ax.plot([xmin, xmax], [mean, mean], label=('mean = ' + f"{mean:.1f}"))
    ax.legend(loc='upper left')
    ax.set_title(denom_comerc + line + grupo)
    ax.grid()
    plt.tight_layout()
    plt.show()


def compare_measure_line_plot(df):
    df= df.sort_index()
    df = df.ffill()
    fig, ax = plt.subplots(1,1, figsize=(16,9), facecolor= 'Grey')
    for i in range(len(df.columns)):
        ax.plot(df.index, df.iloc[:,i], label= df.columns[i].split()[-1])
    months = mdates.MonthLocator((4,7,10))
    month_fmt = mdates.DateFormatter('%m')
    ax.xaxis.set_minor_locator(months)
    ax.xaxis.set_minor_formatter(month_fmt)
    ax.grid(linestyle = '--')
    ax.grid(which='minor', axis='x', linestyle='-.', linewidth=0.4, color='#e0e0e0')
    ax.legend(loc='upper left')
    ax.set_title(df.columns[0].split()[0].upper())
    plt.tight_layout()
    plt.show()

def compare_measure_bar_plot(df):
    df = df.dropna()
    x_indexes = np.arange(len(df))
    width = np.min(np.diff(x_indexes))/len(df.columns)
    fig, ax = plt.subplots(1,1, figsize=(16,9), facecolor='Grey')
    for i in range(len(df.columns)):
        ax.bar(x_indexes+(i*width), df.iloc[:,i].values, width=width*.8, align='edge', label=df.columns[i].split()[-1])
    ax.legend(loc='upper left')
    ax.set_title(df.columns[0].split()[0].upper())
    ax.set_xticks(ticks=x_indexes)
    ax.set_xticklabels(df.index.date, rotation=45, ha='center')
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.set_ylabel('R$(,000)')
    ax.grid(linestyle='--')
    plt.tight_layout()
    plt.show()

