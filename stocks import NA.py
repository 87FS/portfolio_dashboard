import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_datareader as pdr


## connecting to spreadsheets through Google Cloud Service
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(
         r'C:\Users\nauka\Desktop\projekt portfolio\credentials.json', scope)

gc = gspread.authorize(credentials)

wks = gc.open("portfolio").worksheet("transactions")

## getting g-sheet data
data = wks.get_all_values()
headers = data.pop(0)

investments = pd.DataFrame(data, columns=headers)

## fixing comma separated decimals in g-sheets
investments["Purchase Price"] = investments["Purchase Price"].str.replace(",", ".")


## iterating through tickers, to get price history of all of them

stock_prices_dataframes = []
portfolio = []
for index, ticker in investments.iterrows():
    if ticker["Currency"] == "PLN":
        symbol = ticker["Ticker"][:3]
        stock_start_date = pd.to_datetime(ticker["Purchase Date"]
                                    , dayfirst=False
                                    , yearfirst = True)

        out_prices_raw = pdr.stooq.StooqDailyReader(ticker["Ticker"]
                                        , start = stock_start_date
                                        , end = np.datetime64("today")).read()

        ## scraping dividends data, checking if dividends were ever issued
        try:
            url = f"https://strefainwestorow.pl/notowania/gpw/{symbol}/dywidendy"


            out_div_raw = pd.read_html(url, match = "Dywidenda za rok")[0]

            out_div_raw.columns = ( [".", "Date", ".", ".", "Dividends"])
            out_div_raw[["Dividends", "."]] = out_div_raw["Dividends"].str.split(" ", expand = True)
            out_div_raw["Dividends"] = out_div_raw["Dividends"].astype(float)

            out_div_fixed = out_div_raw[["Date", "Dividends"]].copy()
            out_div_fixed["Date"] = out_div_fixed["Date"].astype('datetime64[D]')

            ## column contains the last date to buy stock, but the cut-dividend is the next valid session
            ## one cannot add 1 day since it could show up on the weekend, instead use busday_offset
            out_div_fixed["Date"] = np.busday_offset( np.array(out_div_fixed["Date"]
                                                                , dtype = 'datetime64[D]')
                                                                    , offsets = 1
                                                                    , roll = "forward")

            out_prices_raw = pd.merge(left = out_prices_raw
                                        ,right = out_div_fixed
                                        , on = "Date"
                                        , how = "left")

            out_prices_raw.fillna(0, inplace = True)

            ## ImportError means that there were never dividends issued by the company
        except ImportError:
            ## adding missing column in such situation
            out_prices_raw["Dividends"] = 0.0

        ## adding missing column
        out_prices_raw["Stock Splits"] = 0.0

        ## unifying dataframe end look to the other scraper
        out_prices_raw.set_index("Date", inplace = True)

    else:
        stock = yf.Ticker(ticker["Ticker"])

        stock_start_date = pd.to_datetime(ticker["Purchase Date"]
                                    , dayfirst=False
                                    , yearfirst = True)
        ## downloading price history
        out_prices_raw = stock.history(start = stock_start_date
                                              , end = np.datetime64("today")
                                              , interval = "1d")


    ## adding ticker name
    out_prices_raw["Ticker"] = ticker["Ticker"]

    ## setting ticker name first
    order = [7,0,1,2,3,4,5,6]
    out_prices_raw = out_prices_raw[ [out_prices_raw.columns[i] for i in order] ]

    ## putting dates into column (from index)
    out_prices_raw.reset_index(inplace=True)
    out_prices_raw.rename( columns = {"index" : "Date"}, inplace = True)
    out_prices_raw["Date"] = out_prices_raw["Date"].astype(np.datetime64)
    ## filling missing data (no weekends, etc)
    all_days = pd.date_range(start = stock_start_date
                                , end = np.datetime64("today")
                                , freq = "1d")

    df_all_days = pd.DataFrame(all_days, columns = ["Date"])

    out_prices_fixed = pd.merge(left = df_all_days
                                     , right = out_prices_raw
                                     , on = "Date"
                                     , how = "left")

    ## filling missing data with last proper value
    out_prices_fixed.fillna(method = "ffill", inplace = True)

    ## creating a new df with adjusted stocks amounts
    out_stock_amounts = out_prices_fixed[ ["Date", "Ticker", "Stock Splits"]].copy(deep = True)
    out_stock_amounts["Purchase Price"] = ticker["Purchase Price"]
    out_stock_amounts["Purchase Price"] = out_stock_amounts["Purchase Price"].astype(float)
    ## checking if there were any splits
    if out_stock_amounts["Stock Splits"].sum() > 0:
        (out_stock_amounts["Stock Splits"]
                                .replace(to_replace = 0.0
                                        , method = "ffill"
                                        , inplace = True))
        (out_stock_amounts["Stock Splits"]
                                .replace(to_replace = 0.0
                                        , value = 1
                                        , inplace = True))
        ## adding new columns
        out_stock_amounts["Value Amount"] = ticker["Purchase Amount"]
        out_stock_amounts["Dividend Amount"] = ticker["Purchase Amount"]
        out_stock_amounts["Value Amount"] = out_stock_amounts["Value Amount"].astype(float)
        out_stock_amounts["Dividend Amount"] = out_stock_amounts["Dividend Amount"].astype(float)
        out_stock_amounts["Value Amount"] = out_stock_amounts["Value Amount"] * out_stock_amounts.iloc[-1]["Stock Splits"]
        out_stock_amounts["Dividend Amount"] = out_stock_amounts["Dividend Amount"] * out_stock_amounts["Stock Splits"]
        #out_stock_amounts.drop( columns = "Stock Splits")
    else:
        out_stock_amounts[["Value Amount", "Dividend Amount"]] = ticker["Purchase Amount"]
        out_stock_amounts["Value Amount"] = out_stock_amounts["Value Amount"].astype(float)
        out_stock_amounts["Dividend Amount"] = out_stock_amounts["Dividend Amount"].astype(float)
        #out_stock_amounts.drop( columns = "Stock Splits")

    out_stock_amounts["Total Purchase"] = float(ticker["Purchase Price"]) * float(ticker["Purchase Amount"])
    out_stock_amounts = pd.merge(left = df_all_days
                                     , right = out_stock_amounts
                                     , on = "Date"
                                     , how = "left")

    ## filling missing data with last proper value
    out_stock_amounts.fillna(method = "ffill", inplace = True)
    out_stock_amounts.set_index("Date", inplace = True)
    portfolio.append(out_stock_amounts)


    #stock_prices_dataframes.append(out_prices_fixed)
x = portfolio[1][["Total Purchase", "Value Amount", "Dividend Amount"]].add(portfolio[0][["Total Purchase", "Value Amount", "Dividend Amount"]], fill_value = 0).add(portfolio[2][["Total Purchase", "Value Amount", "Dividend Amount"]], fill_value = 0)
x["Average Price"] = x["Total Purchase"] / x["Value Amount"]
#stocks = pd.concat(stock_prices_dataframes)
#stocks.sort_values(["Date", "Ticker"], inplace=True)
#stocks.drop_duplicates()
#stocks.reset_index(drop = True, inplace=True)
portfolio[0].to_csv("1")
portfolio[1].to_csv("2")
x.to_csv("test.csv")
