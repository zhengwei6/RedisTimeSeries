import pandas as pd
import numpy as np
from matplotlib import pyplot as plt 
from pmdarima.arima import ndiffs
import sys
#from statsmodels.graphics.tsaplots import plot_pacf
from statsmodels.tsa.stattools import acf, pacf



if __name__ == "__main__":



    data = pd.read_csv(sys.argv[1])

    

    d = sys.argv[2]
    #print(d)
    #print(d)
    #lag_acf = acf(data["value"].diff(d).dropna(), nlags=30)
    #lag_pacf = pacf(data["value"].dropna(), nlags=25)

    if d == '0':
        lag_pacf = pacf(data["value"].dropna(), nlags=25)
    else:
        lag_pacf = pacf(data["value"].diff(d).dropna(), nlags=25)

    #print(lag_pacf)
  

    f = open("./ts_analysis/pacf.txt", "w")
    #f = open("pacf.txt", "w")
    for i in lag_pacf:
        f.write(str(i)+'\n')
    f.close()
   
    #plot_pacf(data["value"].diff(d).dropna())
    #lt.savefig("../desktop/for_redis/pacf.png")
    #plt.savefig("pacf.png")

    #  觀察ACF圖，參數是差分之後的資料
    #plot_acf(data["time_imputed"].diff(1))
    #plt.show()


    

 
