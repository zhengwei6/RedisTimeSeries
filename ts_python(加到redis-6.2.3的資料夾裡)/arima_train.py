import pmdarima as pm
from pmdarima import model_selection
from pmdarima import auto_arima
from sklearn.metrics import mean_squared_error
import numpy as np
import pandas as pd
import pickle
import matplotlib.pyplot as plt

input_t = []
f = open("python_read.txt", "r")

l_count = 0
p_start = 0
p_end = 0
q_start = 0
q_end = 0
d = 0
seasonal = 0
N = 0
model_file = ""
result_file = ""

for line in f:
    if l_count == 0:
        p_start = int(line[:-1])
    elif l_count == 1:
        p_end   = int(line[:-1])
    elif l_count == 2:
        q_start = int(line[:-1])
    elif l_count == 3:
        q_end = int(line[:-1])
    elif l_count == 4:
        d    = int(line[:-1])
    elif l_count == 5:
        seasonal = int(line[:-1])
    elif l_count == 6:
        N = int(line[:-1])
    elif l_count == 7:
        model_file = line[:-1]
    elif l_count == 8:
        result_file = line[:-1]
    else:
        line = line[:-1]
        input_t.append(line)
    l_count += 1

if model_file == "":
    model_file = "./arima.pkl"

if result_file == "":
    result_file = "./train_result.jpg"

test  = input_t[-N:]
train = input_t[:-N]

test  = np.array(test)
test  = test.astype(np.float)
train = np.array(train)
train = train.astype(np.float)

f.close()

# # Fit a simple auto_arima model
stepwise_model = auto_arima(train, start_p=p_start, start_q=q_start,
                           max_p=p_end, max_q=q_end, m=12,
                           start_P=0, seasonal=seasonal,
                           d=d, D=d, trace=True,
                           error_action='ignore',  
                           suppress_warnings=True, 
                           stepwise=True)

stepwise_model.aic()
stepwise_model.fit(train)

with open(model_file, 'wb') as pkl:
    pickle.dump(stepwise_model, pkl)
print("\n")
print("write to file: " + model_file + "\n")
print("predict result file: " + result_file + "\n")

future_forecast = stepwise_model.predict(n_periods=test.size)
plt.subplots(dpi=600)
plt.plot([i for i in range(train.size, train.size + test.size)], test, linewidth=0.2, color='blue')
plt.plot([i for i in range(train.size, train.size + test.size)], future_forecast, linewidth=0.2, color='red')
plt.plot([i for i in range(train.size)], train, linewidth=0.2, color='blue')
plt.savefig(result_file)

