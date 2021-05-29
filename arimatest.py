import pmdarima as pm
from pmdarima import model_selection
from sklearn.metrics import mean_squared_error
import numpy as np

# #############################################################################
# Load the data and split it into separate pieces
# data = pm.datasets.load_lynx()
# f = open("test.txt", "w")
# t = 2
# for value in data:
#     f.write(str(t))
#     f.write(" ")
#     f.write(str(value))
#     f.write("\n")
#     t += 1
# f.close()

input_t = []
f = open("python_read.txt", "r")
l_count = 0
p = 0
q = 0
d = 0
n = 0

for line in f:
    if l_count == 0:
        p = int(line[:-1])
    elif l_count == 1:
        q = int(line[:-1])
    elif l_count == 2:
        d = int(line[:-1])
    elif l_count == 3:
        n = int(line[:-1])
    else:
        line = line[:-1]
        input_t.append(line)
    l_count += 1

data2 = np.array(input_t)
data2 = data2.astype(np.float)
train, test = model_selection.train_test_split(data2, train_size=n)

f.close()

# # Fit a simple auto_arima model
modl = pm.auto_arima(train, start_p=p, start_q=q, start_P=p, start_Q=q,
                     max_p=5, max_q=5, max_P=5, max_Q=5, seasonal=True,
                     stepwise=True, suppress_warnings=True, D=d, max_D=d,
                     error_action='ignore')

# Create predictions for the future, evaluate on test
preds, conf_int = modl.predict(n_periods=test.shape[0], return_conf_int=True)

# Print the error:
f = open("python_read.txt", "w")

#print("Test RMSE: %.3f" % np.sqrt(mean_squared_error(test, preds)))
f.write("%.3f" % np.sqrt(mean_squared_error(test, preds)))

f.close()