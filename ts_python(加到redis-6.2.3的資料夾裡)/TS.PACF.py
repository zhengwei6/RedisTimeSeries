#!/usr/bin/env python
# coding: utf-8

# In[4]:


import math
import sys
import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import pacf


# In[5]:


arr = list()

# with open("./stock_price_close_value_last_10000.txt", "r") as fp:
with open(sys.argv[1], "r") as fp:
    arr = fp.readlines()

arr = [float(i) for i in arr]
# arr = [float(i.split()[1]) for i in arr]


# In[6]:


# d = 1
d = int(sys.argv[2])


# In[9]:


if d != 0:
    ans = pacf(pd.Series(arr).diff(d).dropna())
else:
    ans = pacf(pd.Series(arr).dropna())


# In[ ]:


with open(sys.argv[1], 'w') as fp:
    for i in ans:
        fp.write(str(i) + "\n")

