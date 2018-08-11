##
## 
##

import numpy as np # load "numpy"
import pandas as pd # load "numpy"
from pandas import Series, DataFrame

## 1.
import glob # 'glob' searches for files

filelist = glob.glob('*.csv') 

df = pd.DataFrame()
for f in filelist:
    stockname = f.split(sep=".cs")[0]
    newdf = pd.read_csv(f)
    newdf['Stock'] = stockname # Add name of stock to records
    df = pd.concat([df,newdf])

# Make a copy of the data frame
df1 = df.copy()

# Add columns with price*volume for each price, which will be
# used together with 'groupby' to compute terms in the numerator
# of the weighted average formula 
df1['OpenV'] = df1['Open']*df1['Volume']
df1['HighV'] = df1['High']*df1['Volume']
df1['LowV'] = df1['Low']*df1['Volume']
df1['CloseV'] = df1['Close']*df1['Volume']

groupbydate = df1[['OpenV','HighV','LowV','CloseV','Volume']].groupby(df1['Date']) # Group by date
datesums = groupbydate.sum() # Add values by date

# These compute the "Python index" for each of Open, High,
# Low, Close for each date.
datesums['PyOpen'] = datesums['OpenV']/datesums['Volume']
datesums['PyHigh'] = datesums['HighV']/datesums['Volume']
datesums['PyLow'] = datesums['LowV']/datesums['Volume']
datesums['PyClose'] = datesums['CloseV']/datesums['Volume']

# Extract just the columns of interest, the Python index.
pyindex = datesums[['PyOpen','PyHigh','PyLow','PyClose']].copy()
pyindex = pyindex.sort_index(ascending=False) # reverse order of dates

# Extract the records between 2007-01-02 and 2012-12-31
pyindex = pyindex[(pyindex.index <= "2012-12-31") & (pyindex.index >= "2007-01-02")]
print(pyindex) # Print the Python index dataframe

## 2.
from datetime import date, timedelta

# List all dates in 2007-01-02 to 2012-12-31
d1 = date(2007, 1, 2)
d2 = date(2012, 12, 31)

delta = d2 - d1 # Number of days between d1 and d2
alldates = [] # Initialize list of dates
for i in range(delta.days + 1): # Create list of dates
    alldates.append((d1 + timedelta(days=i)).strftime('%Y-%m-%d'))

# Remove Python index dates from alldates, leaving dates market closed
closeddates = np.setdiff1d(alldates,pyindex.index)
cd = pd.Series(closeddates)
print(cd) # Print dates the market is closed

## 3.
stocklist = []  # Generate list of stock symbols
for f in filelist:
    stocklist.append(f.split(sep=".cs")[0])

# Make a fresh copy of the original combined dataframe
df1 = df.copy()

# Open dates across all stocks
opendates = pd.unique(df1['Date'])
missingrecs = pd.DataFrame(columns = df1.columns.values)

for s in stocklist:
    stockdf = df1[df1.Stock == s]
    tradedates = stockdf['Date']
    stockopendates = opendates[(opendates<=tradedates.max()) & (opendates >= tradedates.min())]
    missingdates = np.setdiff1d(stockopendates,tradedates)
    if len(missingdates) > 0:
        print("Processing missing values from",s)
        missdf = pd.DataFrame(columns=df1.columns.values)
        missdf['Date'] = missingdates
        missdf['Stock'] = s
        missdf['Adj Close'] = 0
        for i in range(len(missdf)):
            d = missdf.loc[i,'Date']
            x = sum(stockdf['Date'] > missdf.loc[i,'Date'])
            d2 = stockdf.loc[x-1,'Date']
            d1 = stockdf.loc[x,'Date']
            del2 = (pd.to_datetime(d2) - pd.to_datetime(d)).days
            del1 = (pd.to_datetime(d) - pd.to_datetime(d1)).days
            s1 = np.array(stockdf.loc[x,['Open','High','Low','Close','Volume']])
            s2 = np.array(stockdf.loc[x-1,['Open','High','Low','Close','Volume']])
            new = (del2*s1 + del1*s2)/(del1+del2)
            missdf.loc[i,['Open','High','Low','Close','Volume']]=new
        missingrecs = pd.concat([missingrecs,missdf])
    else:
        print("Nothing missing from ",s)

# Convert the dates back into strings, then join the missing
# record dataframe to the dataframe of known records.
missingrecs['Date'] = missingrecs['Date'].astype(str)
df1 = pd.concat([df1,missingrecs])

# Below we repeat the code from #1, this time with the
# addtional new records from the missing data included.

# Add columns with price*volume for each price, which will be
# used together with 'groupby' to compute terms in the numerator
# of the weighted average formula 
df1['OpenV'] = df1['Open']*df1['Volume']
df1['HighV'] = df1['High']*df1['Volume']
df1['LowV'] = df1['Low']*df1['Volume']
df1['CloseV'] = df1['Close']*df1['Volume']

groupbydate = df1[['OpenV','HighV','LowV','CloseV','Volume']].groupby(df1['Date']) # Group by date
datesums = groupbydate.sum() # Add values by date

# These compute the "Python index" for each of Open, High,
# Low, Close for each date.
datesums['PyOpen'] = datesums['OpenV']/datesums['Volume']
datesums['PyHigh'] = datesums['HighV']/datesums['Volume']
datesums['PyLow'] = datesums['LowV']/datesums['Volume']
datesums['PyClose'] = datesums['CloseV']/datesums['Volume']

# Extract just the columns of interest, the Python index.
pyindex = datesums[['PyOpen','PyHigh','PyLow','PyClose']].copy()
pyindex = pyindex.sort_index(ascending=False) # reverse order of dates

# Extract the records between 2007-01-02 and 2012-12-31
pyindex = pyindex[(pyindex.index <= "2012-12-31") & (pyindex.index >= "2007-01-02")]
print(pyindex) # Print the Python index dataframe







