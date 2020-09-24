# br_stocks
## Documentation coming soon
## Please drop me a note with any feedback you have. diegossilveira@outlook.com

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
 
### Whenever possible, there is the possibility to obtain values per quarter, year to date, or trailing twelve months.
### Several of these data can be plotted, with templates already implemented.
### Other important functions:
* given a ticker traded on the exchange, it is possible to request peer companies;
* generate a dataframe with all indicators for several tickers, facilitating the broad comparison of companies;
* generate dataframe and plot the comparison of a measure for several companies.
