import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_datareader.data as web
import time

json_cred = r'C:\Users\nauka\Desktop\projekt portfolio\credentials.json'
gsheet = "portfolio"
wksheet = "transactions"

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


purchases = gspread_parser()

currencies_history = currency_parser(purchases)

purchases = pd.merge( purchases
                      , currencies_history
                      , how = "inner"
                      , left_on = ["Purchase Date", "Currency"]
                      , right_on = ["Date", "Currency"]
                      )

