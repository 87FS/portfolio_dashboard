import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_datareader.data as web
import time

json_cred = r'C:\Users\nauka\Desktop\projekt portfolio\credentials.json'
gsheet = "portfolio"
wksheet = "transactions2"

def gspread_parser(json_cred = json_cred, spreadsheet = gsheet, worksheet = wksheet):
    ''' fetches the google spreadsheet with the history of stock purchases

        json_cred is path for project auth credentials from Google Cloud Service
        spreadsheet and worksheet are string with names

        spreadsheet must contain columns:
        Ticker = only ticker abbreviation existing on yahoo finance
        Purchase Date = date in format yyyy-mm-dd
        Purchase Price = total price (inc. fees) per stock on the moment of purchase
        Purchase Amount = total amount bought on the moment of purchase
        Currency = short abbreviation (like EUR, HKD) '''
    
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_cred, scope)

    gc = gspread.authorize(credentials)

    wks = gc.open(spreadsheet).worksheet(worksheet)

    ## getting g-sheet data
    data = wks.get_all_values()
    headers = data.pop(0)

    purchases = pd.DataFrame(data, columns=headers)

    ## fixing comma separated decimals in g-sheets
    purchases["Purchase Price"] = purchases["Purchase Price"].str.replace(",", ".")
    purchases["Liquidation Rate"] = purchases["Liquidation Rate"].str.replace(",", ".")
    ## clearing data, unifying values
    for column in purchases.columns:
        purchases[column] = purchases[column].str.strip()
        if column in ["Ticker", "Currency"]:
            purchases[column] = purchases[column].str.upper()
        else:
            purchases[column] = purchases[column].str.title()

    purchases["Purchase Date"] = purchases["Purchase Date"].astype(np.datetime64)

    
    return purchases



def currency_parser(investments):

    ''' fetches historical exchange to PLN
        for every distinct currency in the investments dataframe

        investments is a dataframe containing columns "Date" and "Currency"
        "Currency" column is a short abbreviation of currency name (like USD, EUR, etc)'''
    
    pairs = [currency + "/PLN" for currency in investments["Currency"].unique()]
    currency_start_date = investments["Purchase Date"].min()


    currency_prices_dataframes = []
    for pair in pairs:
        if pair != "PLN/PLN":
            currency_raw = web.DataReader( pair
                                      , "av-forex-daily"
                                      , start = currency_start_date
                                      , end = np.datetime64("today")
                                      , api_key = "PLPX5PE8L7HVWLJ7")
            ## adding column with ticker
            currency_raw["Symbol"] = pair
            currency_raw["Currency"] = pair.replace("/PLN", "")

            ## getting dates out of index
            currency_raw.reset_index(inplace=True)
            currency_raw.rename( columns = {"index" : "Date"}, inplace = True)
            currency_raw["Date"] = currency_raw["Date"].astype(np.datetime64)
            currency_raw.drop(columns = ["open", "high", "low"], inplace = True)
            currency_raw.rename(columns = {"close" : "Exchange Rate to PLN"}, inplace = True)

            currency_raw = currency_raw[["Date", "Symbol", "Currency", "Exchange Rate to PLN"]]

                ## filling missing data (no weekends, etc)
            all_days = pd.date_range(start = currency_start_date
                                , end = np.datetime64("today")
                                , freq = "1d")

            df_all_days = pd.DataFrame(all_days, columns = ["Date"])

            currency_fixed = pd.merge(left = df_all_days
                                     , right = currency_raw
                                     , on = "Date"
                                     , how = "left")

            ## filling missing data with last proper value
            currency_fixed.fillna(method = "ffill", inplace = True)     


        
            currency_prices_dataframes.append(currency_fixed)

            ## overcoming API limits
            time.sleep(40)

        ### DEPRECATED, only useful for polish stock exchange stocks ###
    ## adding pln to pln exchange for the sake of simplicity in Power BI
    #pln_to_pln_dataframe = currency_prices_dataframes[0].copy(deep = True)
    #pln_to_pln_dataframe[["Symbol", "open", "high", "low", "close"]] = ["PLN/PLN", 1,1,1,1]
    #currency_prices_dataframes.append(pln_to_pln_dataframe)


    currencies = pd.concat(currency_prices_dataframes)
    currencies.sort_values(["Date", "Symbol"], inplace = True)
    currencies.reset_index(drop = True, inplace = True)

    return currencies


purchases_history = gspread_parser()

currencies_history = currency_parser(purchases_history)

purchases_history = pd.merge( purchases_history
                      , currencies_history
                      , how = "inner"
                      , left_on = ["Purchase Date", "Currency"]
                      , right_on = ["Date", "Currency"]
                      )

purchases_history.drop(columns = [ "Date", "Symbol" ], inplace = True)


def stock_parser(investments):
    ''' the function takes investments dataframe
        and returns both cleared stocks prices history (with split, but without dividend adjust)
        and portfolio history (all investors possesion per day, with ticker and adjusted amounts)

        investments dataframe must contain columns:
        Ticker = only ticker abbreviation existing on yahoo finance
        Purchase Date = date in format yyyy-mm-dd
        Purchase Price = total price (inc. fees) per stock on the moment of purchase
        Purchase Amount = total amount bought on the moment of purchase
        Currency = short abbreviation (like EUR, HKD) '''
    
    stock_prices_dataframes = [] # this df contains all df with stock prices (before combining)
    unmerged_portfolio_dataframes = [] # this df contains 1 df per each ticker (after combining)

    unique_tickers = [ ticker for ticker in investments["Ticker"].unique().tolist() ]

    for unique_ticker in unique_tickers:
        single_ticker_dataframes = [ ] # this df contains all df for a single ticker (multiple purchases)

        for index, ticker in investments[investments["Ticker"] == unique_ticker].iterrows():

                        ## getting stocks data (date, price, dividends and splits)    
            stock = yf.Ticker(ticker["Ticker"])

            stock_start_date = pd.to_datetime(ticker["Purchase Date"]
                                              , dayfirst=False
                                              , yearfirst = True
                                              )
            ## downloading price history
            prices_raw = stock.history(start = stock_start_date
                                           , end = np.datetime64("today")
                                           , interval = "1d"
                                           , auto_adjust = False # seems like True adjusts backwards for dividends
                                           )


            ## adding ticker name
            prices_raw["Ticker"] = ticker["Ticker"]

            ## clearing the dataframe of useless data

            prices_raw_cleared = prices_raw[ ["Ticker"
                                              , "Close"
                                              , "Dividends"
                                              , "Stock Splits"] ].copy(deep=True)
                                                     
            ## putting dates into column (from index)
            prices_raw_cleared.reset_index(inplace=True)

            prices_raw_cleared.rename(columns = {"index" : "Date",
                                                 "Close" : "Price"}
                                          , inplace = True
                                          )
            
            prices_raw_cleared["Date"] = prices_raw_cleared["Date"].astype(np.datetime64)
            
            ## filling missing data (no weekends, etc)
            all_days = pd.date_range(start = stock_start_date
                                     , end = np.datetime64("today")
                                     , freq = "1d"
                                     )

            df_all_days = pd.DataFrame(all_days
                                       , columns = ["Date"]
                                       )

            prices_fixed = pd.merge(left = df_all_days
                                        , right = prices_raw_cleared
                                        , on = "Date"
                                        , how = "left"
                                        )

            ## filling missing data with last proper value
            prices_fixed.fillna(method = "ffill"
                                    , inplace = True
                                    )





                        ## parsing the spreadsheet portfolio to create portfolio FACT table
            
            stock_amounts = prices_fixed[ ["Date", "Ticker", "Stock Splits"]].copy(deep = True)
##            stock_amounts["Purchase Price"] = ticker["Purchase Price"]
##            stock_amounts["Purchase Price"] = stock_amounts["Purchase Price"].astype(float)
            
            ## checking if there were any splits
            if stock_amounts["Stock Splits"].sum() > 0:

                ## filling missing values in Stock Splits column to have a denominator for amounts
                (stock_amounts["Stock Splits"]
                                        .replace(to_replace = 0.0
                                                , method = "ffill"
                                                , inplace = True))
                (stock_amounts["Stock Splits"]
                                        .replace(to_replace = 0.0
                                                , value = 1
                                                , inplace = True))
                
                ## adding new columns with proper amount of stocks (split adjusted)
                stock_amounts["Value Amount"] = float(ticker["Purchase Amount"])
                stock_amounts["Dividend Amount"] = float(ticker["Purchase Amount"])
                
                stock_amounts["Value Amount"] = (stock_amounts["Value Amount"]
                                                             *
                                                     stock_amounts.iloc[-1]["Stock Splits"] ## [-1] has the split value
                                                     )
                stock_amounts["Dividend Amount"] = (stock_amounts["Dividend Amount"]
                                                                *
                                                        stock_amounts["Stock Splits"]
                                                        )
                
                
            else:
                stock_amounts[["Value Amount", "Dividend Amount"]] = ticker["Purchase Amount"]
                stock_amounts["Value Amount"] = stock_amounts["Value Amount"].astype(float)
                stock_amounts["Dividend Amount"] = stock_amounts["Dividend Amount"].astype(float)
                

            stock_amounts["Total Purchase in PLN"] = (float(ticker["Purchase Price"])
                                                      *
                                                      float(ticker["Purchase Amount"])
                                                      *
                                                      float(ticker["Exchange Rate to PLN"]))
            
            stock_amounts = pd.merge(left = df_all_days
                                             , right = stock_amounts
                                             , on = "Date"
                                             , how = "left")
            
            ## filling missing data with last proper value
            stock_amounts.fillna(method = "ffill", inplace = True)
            stock_amounts.drop(columns = "Stock Splits", inplace = True)

            single_ticker_dataframes.append(stock_amounts)
            stock_prices_dataframes.append(prices_fixed)

        if len(single_ticker_dataframes) == 1:
            single_ticker_dataframes[0]["Average Price in PLN"] = round(
                                                                        (
                                                                            single_ticker_dataframes[0]["Total Purchase in PLN"]
                                                                            /
                                                                            single_ticker_dataframes[0]["Value Amount"])
                                                                        , 2)
            unmerged_portfolio_dataframes.append(single_ticker_dataframes[0])

        else:
        ## combining all purchases of the same ticker into one dataframe        
            base = single_ticker_dataframes[0].copy(deep = True)
            for i in range(1, len(single_ticker_dataframes)):
                base[["Total Purchase in PLN", "Value Amount", "Dividend Amount"]] = (base[["Total Purchase in PLN", "Value Amount", "Dividend Amount"]]
                                                                                      .add
                                                                                      (single_ticker_dataframes[i][["Total Purchase in PLN", "Value Amount", "Dividend Amount"]]
                                                                                       , fill_value = 0 ))

            base["Average Price in PLN"] = round(base["Total Purchase in PLN"] / base["Value Amount"], 2)
            unmerged_portfolio_dataframes.append(base)


    stocks = pd.concat(stock_prices_dataframes)
    stocks.sort_values(["Date", "Ticker"], inplace = True)
    stocks.drop_duplicates(inplace=True)
    stocks.reset_index(drop = True, inplace=True)

    portfolio = pd.concat(unmerged_portfolio_dataframes)
    portfolio.sort_values(["Date", "Ticker"], inplace = True)
    portfolio.reset_index(drop = True, inplace=True)

    return stocks, portfolio


stocks_history, portfolio_history = stock_parser(purchases_history)
