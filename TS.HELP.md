### TS.HELP
```html
TS.LOAD key filename
    load samples from the file to the key (key must be created first)
Arguments
    - filename: file name 
Example
    TS.LOAD foo ./bar.txt
Notes
    The format of file is:
    <timestamp1> <value1>
    <timestamp2> <value2>
    <timestamp3> <value3>
------------------------------------------------------------------------------------------------------------------
TS.PRINT key
    print samples in the key
------------------------------------------------------------------------------------------------------------------
TS.ANALYSIS key function [order_of_difference]
    analyze samples in the key
Arguments
    - function: ndiffs, pacf, acf
        - ndiffs: calculate order of difference suits for the samples in a key
        - pacf: calculate the partial autocorrelation function of the samples in a key
        - acf: calculate the autocorrelation function of the samples in a key
Optional arguments (for pacf and acf this is a must)
    - order_of_difference: 0, 1, 2
Example
    TS.ANALYSIS foo ndiffs
    TS.ANALYSIS foo pacf 1
    TS.ANALYSIS foo acf 0
------------------------------------------------------------------------------------------------------------------
TS.TRAIN key P p_start p_end D d Q q_start q_end [SEASONAL seasonal] [N number_of_testing_samples] [M model_file] [R result_image]
    train an auto_arima model to predict the future value of samples in the key and save the model parameters in a file
Arguments
    - p_start: start value of p for auto_arima model 
    - p_end: max value of p auto_arima model will test 
    - d: order of difference
    - q_start: start value of q for auto_arima model
    - q_end: max value of q auto_arima model will test
Optional arguments
    - seasonal: 0, 1
        - 0: for non-seasonal data
        - 1: for seasonal data
        - Default: 0
    - number_of_testing_samples: number of testing samples
        - Default: 0
    - model_file: file name of the file containing parameters of the trained model
        - Default: arima.pkl
    - result_image: line graph of comparison of ground truth (blue line) and predicted values (red line)
        - Default: train_result.jpg
Example
    TS.TRAIN foo P 1 3 D 1 Q 1 3 SEASONAL 1 N 100 M arima_model.pkl R train_result.jpg
    (suppose number of samples is 1000, train auto_arima model with training data of size 900 and testing data of size 100)
------------------------------------------------------------------------------------------------------------------
TS.PREDICT key [N number_of_predicted_value] [M model_file] [R result_image]
    predict future values using the trained model
Optional arguments
    - number_of_predicted_value: number of future values to be predicted
        - Default: 0
    - model_file: file name of the file containing parameters of the trained model
        - Default: ./arima.pkl
    - result_image: line graph of the predicted value
        - Default: predict_result.jpg
Example
    TS.PREDICT foo N 100 M arima_model.pkl R predict_result.jpg
    (predict the next 100 values)
------------------------------------------------------------------------------------------------------------------
TS.PLOT key function [JPG jpg]
    plot the specific line graph of samples in the key
Arguments
    - function: data, acf, pacf, diff_acf, diff_pacf, smooth, downsampling
        - data: line graph of the origin data
        - acf: line graph of the autocorrelation function of data
        - pacf: line graph of the partial autocorrelation function of data
        - diff_acf: line graph of the autocorrelation function of the differenced data
        - diff_pacf: line graph of the partial autocorrelation function of the differenced data
        - smooth: line graph of the average mean of data with differnce size of window
        - downsampling: line graph of downsampling of data
Optional arguments
    - jpg: file name of line graph
        - Default: <key>_<function>.jpg
Example
    TS.PLOT foo downsampling JPG foo.jpg
------------------------------------------------------------------------------------------------------------------
TS.ACF key order_of_difference
    plot the autocorrelation function of data on terminal
Arguments
    - order_of_difference: number of difference before calculating autocorrelation function
Example
    TS.ACF foo 1
------------------------------------------------------------------------------------------------------------------
TS.PACF key order_of_difference
    plot the partial autocorrelation function of data on terminal
Arguments
    - order_of_difference: number of difference before calculating partial autocorrelation function
Example
    TS.PACF foo 0
------------------------------------------------------------------------------------------------------------------
TS.DOWNSAMPLING key [INTERVAL interval] [NEWKEY newkey]
     downsampling samples in the key and save the result to a new key (the key cannot be created first)
Arguments
    (none)
Optional arguments
    - interval: sampling rate
        - Default: 5
    - newkey: name of the new key
        - Default: <key>_downsampling_<interval>
Example
    TS.DOWNSAMPLING foo INTERVAL 10 NEWKEY bar
    (downsample samples in foo with sampling rate of 10 and save the result in new key bar)
-----------------------------------------------------------------------------------------------------------------
