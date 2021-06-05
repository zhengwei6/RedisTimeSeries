#!/usr/bin/env python
# coding: utf-8

# In[13]:


import sys
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# In[14]:


arr = list()

# with open("./stock_price_close_value_last_10000.txt", "r") as fp:
with open(sys.argv[1], "r") as fp:
    arr = fp.readlines()

arr = [float(i) for i in arr]
# arr = [float(i.split()[1]) for i in arr]


# In[15]:


size = [i for i in [2**i for i in range(15)][1:] if i < len(arr)/2][-5:]


# In[16]:


arr_roll = list()
for i in size:
    arr_roll.append(np.array(pd.Series(np.append(np.append([0 for j in range(i//2)], arr), [0 for j in range(math.ceil(i/2)-1)])).rolling(window=i, closed='both').mean().dropna()))


# In[17]:


timestamp = [i for i in range(len(arr))]


# In[20]:


fig, axs = plt.subplots(len(size), figsize=(10,10), dpi=300)
fig.tight_layout()
for i in range(len(size)):
    axs[i].plot(timestamp, arr_roll[i])
    axs[i].set_title("moving average window size: " + str(size[i]))
plt.savefig(sys.argv[2])
# plt.show()


# In[ ]:





# In[ ]:




