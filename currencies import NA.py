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

    return purchases

## getting all currencies exchange rates

def currency_parser(investments):

    ''' fetches historical exchange to PLN
        for every distinct currency in the investment dataframe'''
    
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

            ## setting ticker name first
            order = [4,0,1,2,3]
            currency_raw = currency_raw[ [currency_raw.columns[i] for i in order] ]

            ## getting dates out of index
            currency_raw.reset_index(inplace=True)
            currency_raw.rename( columns = {"index" : "Date"}, inplace = True)
            currency_raw["Date"] = currency_raw["Date"].astype(np.datetime64)
            currency_raw.drop(columns = ["open", "high", "low"], inplace = True)
            currency_raw.rename(columns = {"close" : "Close"}, inplace = True)

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


    ## adding pln to pln exchange for the sake of simplicity,
    ## yeah it's always 1, but it makes life easier
    #pln_to_pln_dataframe = currency_prices_dataframes[0].copy(deep = True)
    #pln_to_pln_dataframe[["Symbol", "open", "high", "low", "close"]] = ["PLN/PLN", 1,1,1,1]
    #currency_prices_dataframes.append(pln_to_pln_dataframe)


    currencies = pd.concat(currency_prices_dataframes)
    
    currencies.sort_values(["Date", "Symbol"], inplace = True)
    currencies.reset_index(drop = True, inplace = True)

    return currencies

currencies_history = currency_parser(gspread_parser())
