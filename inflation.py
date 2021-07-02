import numpy as np
import pandas as pd

main = r'https://stat.gov.pl/download/'
raport = r'gfx/portalinformacyjny/pl/defaultstronaopisowa/4741/1/1/'
file = r'miesieczne_wskazniki_cen_towarow_i_uslug_konsumpcyjnych_od_1982_roku.xlsx'

link = main+raport+file

## getting polish CPI data, the file is permanent and updated every month
df = pd.read_excel(
                link
                , header = 0
                , usecols = [2,3,4,5]
                )

## slicing just the month to month cpi comparison
formatted = df[df["Sposób prezentacji"] == "Poprzedni miesiąc = 100"].copy(deep=True)
formatted.drop(columns = ["Sposób prezentacji"], inplace = True)
formatted.rename(columns = {"Rok":"Year", "Miesiąc":"Month", "Wartość":"CPIm2m"}, inplace = True)

## combining columns with separate parts to get date column
formatted["Date"] = formatted["Year"].astype('str') + "-" + formatted["Month"].astype('str') + "-1"
formatted["Date"] = formatted["Date"].astype(np.datetime64)
formatted.sort_values(by = ["Year", "Month"], inplace = True, ignore_index = True)


## here is the issue whether to count current month inflation or not, for now I decided not to
start_date = "2019-04-08" ## placeholder, it will be the min of all dates in purchases spreadsheet
cpi = formatted[formatted["Date"] >= start_date][["Date", "CPIm2m"]].copy(deep = True)
cpi.reset_index(drop = True, inplace=True)
cpi.dropna(inplace = True)


