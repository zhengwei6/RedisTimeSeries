#!/usr/bin/env python
# coding: utf-8

# In[5]:


import sys
import matplotlib.pyplot as plt


# In[6]:


arr = list()

with open(sys.argv[1], "r") as fp:
    arr = fp.readlines()

arr = [float(i) for i in arr]


# In[ ]:


timestamp = [i for i in range(len(arr))]


# In[ ]:


plt.plot(timestamp, arr)
# plt.title('Stock close value')
# plt.xlabel('Time')
plt.ylabel('value')
# plt.show()
plt.savefig(sys.argv[2])

