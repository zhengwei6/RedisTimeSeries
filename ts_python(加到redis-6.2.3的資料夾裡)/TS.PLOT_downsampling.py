#!/usr/bin/env python
# coding: utf-8

# In[16]:


import sys
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import signal


# In[17]:


arr = list()

# with open("./stock_price_close_value_last_10000.txt", "r") as fp:
with open(sys.argv[1], "r") as fp:
    arr = fp.readlines()

arr = [float(i) for i in arr]
# arr = [float(i.split()[1]) for i in arr]


# In[38]:


size = [i for i in [2**i for i in range(15)][1:] if i < len(arr)/10][-5:]


# In[40]:


arr_downsampling = list()
for i in size:
    arr_downsampling.append(signal.resample(arr, len(arr)//i).flatten())


# In[41]:


arr_downsampling[0].shape[0]


# In[42]:


fig, axs = plt.subplots(len(size), figsize=(10,10), dpi=300)
fig.tight_layout()
for i in range(len(size)):
    axs[i].plot([i for i in range(arr_downsampling[i].shape[0])], arr_downsampling[i])
    axs[i].set_title("downsampling frequency: " + str(size[i]))
plt.savefig(sys.argv[2])
# plt.savefig("test_downsamlping.jpg")

