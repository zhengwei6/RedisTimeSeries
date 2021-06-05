import pandas as pd
import numpy as np
from matplotlib import pyplot as plt 
from pmdarima.arima import ndiffs
import sys
from statsmodels.tsa.stattools import acf, pacf




if __name__ == "__main__":



    data = pd.read_csv(sys.argv[1])

    d = sys.argv[2]
    #print(d)
    if d == '0':
        lag_acf = acf(data["value"].dropna(), nlags=25)
    else:
        lag_acf = acf(data["value"].diff(d).dropna(), nlags=25)

    f = open("./ts_analysis/acf.txt", "w")
    #f = open("acf.txt", "w")
    #f = open("pacf.txt", "w")
    for i in lag_acf:
        f.write(str(i)+'\n')
    f.close()
   
    #plot_pacf(data["value"].diff(d))
    #plt.savefig("../desktop/for_redis/pacf.png")

    #  觀察ACF圖，參數是差分之後的資料
    #plot_acf(data["value"].diff(d).dropna())
    #plt.savefig("../desktop/for_redis/acf.png")
    #plt.savefig("acf.png")
    #plt.show()


    

 
