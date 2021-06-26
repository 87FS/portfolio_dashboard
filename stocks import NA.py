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
## unifying tickers
investments["Ticker"] = investments["Ticker"].str.upper()

## iterating through tickers, to get price history of all of them

stock_prices_dataframes = []
portfolio = []

unique_tickers = [ ticker for ticker in investments["Ticker"].unique().tolist() ]

for unique_ticker in unique_tickers:
    single_ticker_dataframes = [ ]

    for index, ticker in investments[investments["Ticker"] == unique_ticker].iterrows():
        print(ticker["Ticker"])

                    ## getting stocks data (date, price, dividends and splits)    
        stock = yf.Ticker(ticker["Ticker"])

        stock_start_date = pd.to_datetime(ticker["Purchase Date"]
                                          , dayfirst=False
                                          , yearfirst = True
                                          )
        ## downloading price history
        out_prices_raw = stock.history(start = stock_start_date
                                       , end = np.datetime64("today")
                                       , interval = "1d"
                                       , auto_adjust = False # seems like True adjusts backwards for dividends
                                       )


        ## adding ticker name
        out_prices_raw["Ticker"] = ticker["Ticker"]

        ## clearing the dataframe of useless data

        out_prices_raw_cleared = out_prices_raw[ ["Ticker"
                                                 , "Close"
                                                 , "Dividends"
                                                 , "Stock Splits"] ].copy(deep=True)
                                                 
        ## putting dates into column (from index)
        out_prices_raw_cleared.reset_index(inplace=True)

        out_prices_raw_cleared.rename(columns = {"index" : "Date",
                                                 "Close" : "Price"}
                                      , inplace = True
                                      )
        
        out_prices_raw_cleared["Date"] = out_prices_raw_cleared["Date"].astype(np.datetime64)
        
        ## filling missing data (no weekends, etc)
        all_days = pd.date_range(start = stock_start_date
                                 , end = np.datetime64("today")
                                 , freq = "1d"
                                 )

        df_all_days = pd.DataFrame(all_days
                                   , columns = ["Date"]
                                   )

        out_prices_fixed = pd.merge(left = df_all_days
                                    , right = out_prices_raw_cleared
                                    , on = "Date"
                                    , how = "left"
                                    )

        ## filling missing data with last proper value
        out_prices_fixed.fillna(method = "ffill"
                                , inplace = True
                                )





                    ## parsing the spreadsheet portfolio to create portfolio FACT table
        
        out_stock_amounts = out_prices_fixed[ ["Date", "Ticker", "Stock Splits"]].copy(deep = True)
        out_stock_amounts["Purchase Price"] = ticker["Purchase Price"]
        out_stock_amounts["Purchase Price"] = out_stock_amounts["Purchase Price"].astype(float)
        
        ## checking if there were any splits
        if out_stock_amounts["Stock Splits"].sum() > 0:

            ## filling missing values in Stock Splits column to have a denominator for amounts
            (out_stock_amounts["Stock Splits"]
                                    .replace(to_replace = 0.0
                                            , method = "ffill"
                                            , inplace = True))
            (out_stock_amounts["Stock Splits"]
                                    .replace(to_replace = 0.0
                                            , value = 1
                                            , inplace = True))
            
            ## adding new columns with proper amount of stocks (split adjusted)
            out_stock_amounts["Value Amount"] = float(ticker["Purchase Amount"])
            out_stock_amounts["Dividend Amount"] = float(ticker["Purchase Amount"])
            
            out_stock_amounts["Value Amount"] = (out_stock_amounts["Value Amount"]
                                                         *
                                                 out_stock_amounts.iloc[-1]["Stock Splits"] ## [-1] has the split value
                                                 )
            out_stock_amounts["Dividend Amount"] = (out_stock_amounts["Dividend Amount"]
                                                            *
                                                    out_stock_amounts["Stock Splits"]
                                                    )
            
            #out_stock_amounts.drop( columns = "Stock Splits")
        else:
            out_stock_amounts[["Value Amount", "Dividend Amount"]] = ticker["Purchase Amount"]
            out_stock_amounts["Value Amount"] = out_stock_amounts["Value Amount"].astype(float)
            out_stock_amounts["Dividend Amount"] = out_stock_amounts["Dividend Amount"].astype(float)
            #out_stock_amounts.drop( columns = "Stock Splits")

        out_stock_amounts["Total Purchase"] = (float(ticker["Purchase Price"])
                                                       *
                                               float(ticker["Purchase Amount"]))
        
        out_stock_amounts = pd.merge(left = df_all_days
                                         , right = out_stock_amounts
                                         , on = "Date"
                                         , how = "left")
        
        ## filling missing data with last proper value
        out_stock_amounts.fillna(method = "ffill", inplace = True)
        out_stock_amounts.set_index("Date", inplace = True)
        
        single_ticker_dataframes.append(out_stock_amounts)
        stock_prices_dataframes.append(out_prices_fixed)


    if len(single_ticker_dataframes) == 1:
        portfolio.append(single_ticker_dataframes[0])
    else:
        base = single_ticker_dataframes[0].copy(deep = True)
        for i in range(1, len(single_ticker_dataframes)):
                      base[["Total Purchase", "Value Amount", "Dividend Amount"]] = base[["Total Purchase", "Value Amount", "Dividend Amount"]].add(single_ticker_dataframes[i][["Total Purchase", "Value Amount", "Dividend Amount"]], fill_value = 0 )
        base["Average Price"] = base["Total Purchase"] / base["Value Amount"]
        portfolio.append(base)
#####stocks = pd.concat(stock_prices_dataframes)
###stocks.sort_values(["Date", "Ticker"], inplace=True)
###stocks.drop_duplicates()
###stocks.reset_index(drop = True, inplace=True)
###portfolio[0].to_csv("1")
###portfolio[1].to_csv("2")
###x.to_csv("test.csv")
