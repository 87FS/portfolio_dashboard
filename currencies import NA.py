import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_datareader.data as web
import time

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

## getting all currencies exchange rates

pairs = [currency + "/PLN" for currency in investments["Currency"].unique()]
currency_start_date = investments["Purchase Date"].min()


currency_prices_dataframes = []
for pair in pairs:
    if pair != "PLN/PLN":
        out_currency_raw = web.DataReader( pair
                                  , "av-forex-daily"
                                  , start = currency_start_date
                                  , end = np.datetime64("today")
                                  , api_key = "PLPX5PE8L7HVWLJ7")
        ## adding column with ticker
        out_currency_raw["Symbol"] = pair

        ## setting ticker name first
        order = [4,0,1,2,3]
        out_currency_raw = out_currency_raw[ [out_currency_raw.columns[i] for i in order] ]

        ## getting dates out of index
        out_currency_raw.reset_index(inplace=True)
        out_currency_raw.rename( columns = {"index" : "Date"}, inplace = True)
        out_currency_raw["Date"] = out_currency_raw["Date"].astype(np.datetime64)

            ## filling missing data (no weekends, etc)
        all_days = pd.date_range(start = currency_start_date
                            , end = np.datetime64("today")
                            , freq = "1d")

        df_all_days = pd.DataFrame(all_days, columns = ["Date"])

        out_currency_fixed = pd.merge(left = df_all_days
                                 , right = out_currency_raw
                                 , on = "Date"
                                 , how = "left")

        ## filling missing data with last proper value
        out_currency_fixed.fillna(method = "ffill", inplace = True)     


    
        currency_prices_dataframes.append(out_currency_fixed)

        ## overcoming rid of API limits
        time.sleep(30)


## adding pln to pln exchange for the sake of simplicity,
## yeah it's always 1, but it makes life easier
pln_to_pln_dataframe = currency_prices_dataframes[0].copy(deep = True)
pln_to_pln_dataframe[["Symbol", "open", "high", "low", "close"]] = ["PLN/PLN", 1,1,1,1]
currency_prices_dataframes.append(pln_to_pln_dataframe)


currencies = pd.concat(currency_prices_dataframes)
currencies.sort_values(["Date", "Symbol"], inplace = True)
currencies.reset_index(drop = True, inplace = True)
