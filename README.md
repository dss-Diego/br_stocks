# Brazilian Stocks Data and Analysis
### P.S.
Please drop me a note with any feedback you have.
diegossilveira@outlook.com

All the results of this script are for information purposes and are not intended to make investment suggestions.

# Project Description

### This project performs several tasks, such as:
* extracts, processes, and loads data from public companies traded on B3 -BOLSA BALCÃO BRASIL in a sqlite3 database;
* extracts, processes, and loads daily prices and number of shares of these companies;
* prepares the following financial statements in Pandas dataframes:
  * income statement
  * balance sheet
  * cash flow statement
* elaborates in Pandas dataframes or Pandas series the following measures:
  * net income
  * ebit
  * depreciation and amortization
  * ebitda
  * revenue
  * cash and cash equivalents
  * total debt
  * net debt
  * market value
  * earnings per share
  * price / earnings
  * total equity
  * total assets
  * return on equity (ROE)
  * return on assets (ROA)
  * debt to equity
  * financial leverage (total assets / total equity)
  * current ratio
  * gross profit margin
  * net profit margin
  * ebitda margin
  * enterprise value
  * enterprise value / ebitda
  * enterprise value / ebit
  * book value per share
  * price / book value per share
  * compound annual growth rate of net income
  * compound annual growth rate of revenue
  * cash flow from operations (CFO)
 
#### Whenever possible, there is the possibility to obtain values per quarter, year to date, or trailing twelve months.
#### Several of these data can be plotted, with templates already implemented.
#### Other important functions:
* given a ticker traded on the exchange, it is possible to request peer companies;
* generate a dataframe with all indicators for several tickers, facilitating the broad comparison of companies;
* generate dataframe and plot the comparison of a measure for several companies.

## Installation:
Be aware that the first time you run this script, all data regarding companies and prices will be downloaded, which takes some time. The next time you run this script, it will update the database with only new or updated data. **Often, existing past data is updated by the CVM.**

1. Clone the repository `git clone https://github.com/dss-Diego/br_stocks.git`

2. Go to the folder ` cd br_stocks`

3. Install requirements `pip install -r requirements.txt`

4. Run `analysis.py`

## Usage:

Creating Class Objects and calling methods

```python
# create the lren3 object that stores attributes of the LREN3 ticker
lren3 = Ticker('lren3')

# call methods 
# the following methods have very similar behaviour:
income_statement = lren3.income_statement(quarter=False, ytd=True, ttm=False, start_period='2018-01-01')
balance_sheet = lren3.balance_sheet(start_period="2018-01-01", plot=True)
cash_flow = lren3.cash_flow(quarter=False, ytd=True, ttm=True, start_period="2018-01-01")
ni = lren3.net_income(quarter=True, ytd=True, ttm=True, start_period="2018", plot=True)
ebit = lren3.ebit(quarter=True, ytd=True, ttm=True, start_period="2018-01-01", plot=True)
depre = lren3.depre_amort(quarter=True, ytd=True, ttm=True, start_period="2018-01-01", plot=True)
ebitda = lren3.ebitda(quarter=True, ytd=True, ttm=True, start_period="2018-01-01", plot=True)
revenue = lren3.revenue(quarter=True, ytd=True, ttm=True, start_period="2018-01-01", plot=True)
cash_equi = lren3.cash_equi(start_period="2018-01-01", plot=True)
total_debt = lren3.total_debt(start_period="all", plot=True)
mv = lren3.market_value(start_period="2018-01-01", plot=True)
net_debt = lren3.net_debt(start_period="all", plot=True)
eps = lren3.eps(start_period="2018-01-01")
p_e = lren3.price_earnings(start_period="2018-01-01-", plot=True)
equity = lren3.total_equity(start_period="all", plot=True)
assets = lren3.total_assets(start_period="all", plot=True)
roe = lren3.roe(start_period="all", plot=True)
roa = lren3.roa(start_period="all", plot=True)
debt_to_equity = lren3.debt_to_equity(start_period="last")
financial_leverage = lren3.financial_leverage(start_period="all")
current_ratio = lren3.current_ratio(start_period="all")
gross_profit_margin = lren3.gross_profit_margin(quarter=True, ytd=True, ttm=True, start_period="2018-01-01")
net_profit_margin = lren3.net_profit_margin(quarter=True, ytd=True, ttm=True, start_period="all")
ebitda_margin = lren3.ebitda_margin(quarter=True, ytd=True, ttm=True, start_period="all")
ev = lren3.enterprise_value(start_period="2018-01-01-", plot=True)
ev_ebitda = lren3.ev_ebitda(start_period="2018-01-01", plot=True)
ev_ebit = lren3.ev_ebit(start_period="2018-01-01", plot=True)
bv_share = lren3.bv_share(start_period="2018-01-01")
p_bv = lren3.price_bv(start_period="2017", plot=True)
cagr_net_income = lren3.cagr_net_income(n_years=5)
cagr_revenue = lren3.cagr_revenue(n_years=5)
cfo = lren3.cfo(quarter=True, ytd=True, ttm=True, start_period="all", plot=True)

# to easily create a list of peer companies:
lren3_peers = lren3.get_peers()

# the following method will return a dataframe with metrics for all tickers in a list
lren3_metrics = Ticker.statistics([lren3])
# it is possible to pass class objects or strings:
compare_metrics = Ticker.statistics([lren3, 'guar3', 'cgra4'])

# to compare a measure between several companies:
compare_measure = Ticker.compare_measure('price_bv', ['wege3', 'egie3', 'lren3', 'vvar3'], {'start_period': '2017'})

```
## Some example images:
![Assets](/imgs/Figure_1.png)

![Net Income](/imgs/Figure_3.png)

![price earnings](/imgs/Figure_12.png)

![ROE](/imgs/Figure_15.png)

![comparison](/figs/Figure_22.png)

 
## Please drop me a note with any feedback you have. diegossilveira@outlook.com
