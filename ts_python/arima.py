import pmdarima as pm
from pmdarima import model_selection
from pmdarima import auto_arima
from sklearn.metrics import mean_squared_error
import numpy as np

import pickle

input_t = []
f = open("python_read.txt", "r")

l_count = 0
p_start = 0
p_end = 0
q_start = 0
q_end = 0
d = 0
seasonal = 0
model_file = ""

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
        model_file = line[:-1]
    else:
        line = line[:-1]
        input_t.append(line)
    l_count += 1

if model_file == None:
    model_file = "./arima.pkl"

train = np.array(input_t)
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

with open(model_file, 'wb') as pkl:
    pickle.dump(stepwise_model, pkl)
print("\n")
print("write to file: " + model_file + "\n")