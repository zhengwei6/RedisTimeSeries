#!/usr/bin/env python
# coding: utf-8

# In[11]:


import sys
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_pacf, plot_acf


# In[12]:


arr = list()

with open(sys.argv[1], "r") as fp:
    arr = fp.readlines()

arr = [float(i) for i in arr]


# In[13]:


timestamp = [i for i in range(len(arr))]


# In[19]:


output_pacf = plot_pacf(pd.Series(arr).dropna())


# In[20]:


output_pacf.savefig(sys.argv[2])


# In[ ]:




