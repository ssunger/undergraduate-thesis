
# coding: utf-8

# In[51]:

import pynance as pn
import pandas as pd
import quandl as qdl
import os
from inspect import getsourcefile
from os.path import abspath
import re,os
import pandas_datareader as pdr
import pynance as pn
import pandas as pd
import os
import pandas_datareader
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import BDay
from pandas.tseries.offsets import CustomBusinessDay

def dates_to_calc(beg_d, end_d, per):
    us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    print(pd.DatetimeIndex(start=beg_d,end=end_d, freq=us_bd))


# In[104]:

qdl.ApiConfig.api_key = ""

def file_iterator(rootdir):
    files = []
    f = ""
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            if file[-3:] == "csv":
                pathers = os.path.join(subdir, file)
                f += pathers + "----"
    return(f[:-4].split("----"))

def get_prices_for_index(beg_d, end_d):
    pl = abspath(getsourcefile(lambda:0))
    cd = re.split(r'\<', pl)[0]
    tickers = []
    t = 0
    for j in file_iterator(cd):
        tickers = pd.read_csv(j)["co_tic"]
        tickers = tickers.tolist()
        for i in tickers:
            t += 1
            if t == 1 :
                data_use = qdl.get_table("WIKI/PRICES",
                               qopts={"columns":["date","ticker","low","high","close","adj_close","volume"]},
                               ticker= i, 
                               date = {'gte': beg_d,'lte' : end_d})
                data_use = data_use.set_index(["date"], drop=True)    

            else:
                temp = qdl.get_table("WIKI/PRICES",
                               qopts={"columns":["date","ticker","low","high","close","adj_close","volume"]},
                               ticker= i, 
                               date = {'gte': beg_d,'lte' : end_d})
                temp = temp.set_index(["date"], drop=True)
               
                tot = [data_use, temp]
                data_use = pd.concat(tot)
    
    #data_use.to_csv("index_constituents_data.csv", sep = ",")
    return data_use


# In[100]:

beg_d = "2016-01-01"
end_d = "2017-12-06"
data_wanted = get_prices_for_index(beg_d,end_d) 


# In[102]:

data_wanted.to_csv("index_constituents_data.csv", sep = ",")


# In[103]:

data_wanted


# In[ ]:



