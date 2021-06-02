import pandas as pd
import numpy as np
from matplotlib import pyplot as plt 
from pmdarima.arima import ndiffs
import sys



if __name__ == "__main__":

##我們的data是txt檔??

    data = pd.read_csv(sys.argv[1])
    #data = pd.read_table('test.txt', delim_whitespace=True, names=('timestamp','value'))
    #data = pd.read_table('./test.txt', names=('value'))
    #print(data["value"])
# 算出推薦的差分次數
    d =  ndiffs(data["value"].dropna(),  test="adf")
    #print("d = ", d) 

    f = open("./ts_analysis/ndiffs.txt", "w")
    f.write(str(d))
    f.close()


    

 
