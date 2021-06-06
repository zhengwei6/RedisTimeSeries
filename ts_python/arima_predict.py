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

N = 0
model_file = ""
result_file = ""
l_count = 0

for line in f:
    if l_count == 0:
        N = int(line[:-1])
    elif l_count == 1:
        model_file = line[:-1]
    elif l_count == 2:
        result_file = line[:-1]
    else:
        line = line[:-1]
        input_t.append(line)
    l_count += 1

if model_file == "":
    model_file = "./arima.pkl"

if result_file == "":
    result_file = "./predict_result.jpg"

input_t = np.array(input_t)
input_t = input_t.astype(np.float)

f.close()
pickle_preds = []
with open(model_file, 'rb') as pkl:
    model = pickle.load(pkl)
    model.fit(input_t)
    pickle_preds = model.predict(n_periods=N)

plt.subplots(dpi=600)
plt.plot([i for i in range(input_t.size)], input_t, linewidth=0.2, color='blue')
plt.plot([i for i in range(input_t.size, input_t.size + N)], pickle_preds, linewidth=0.2, color='red')
plt.savefig(result_file)
print(pickle_preds)
print("\n")
print("predict result file: " + result_file + "\n", flush = True)
print("-------------------------------------------\n")