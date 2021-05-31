#!/usr/bin/env python
# coding: utf-8

# In[25]:


import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_pacf, plot_acf


# In[11]:


arr = list()

with open(sys.argv[1], "r") as fp:
    arr = fp.readlines()

arr = [float(i) for i in arr]


# In[21]:


arr_diff = np.array(pd.Series(arr).diff(1).dropna())


# In[ ]:


output_pacf = plot_pacf(pd.Series(arr_diff).dropna())


# In[ ]:


output_pacf.savefig(sys.argv[2])


# In[ ]:





# In[ ]:




