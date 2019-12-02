Anomaly Detection

We have two datasets regarding this problem, HDFS and BGL, the input is log event sequence (a sequence of log events) in time series and corresponding labels, here the logs are already parsed into different log events, for example [1,2,1,4,5,6,7], it represents a log sequence of event 1,2,1,4,5,6,7. 

parameter setting:
dataPath: the input log event sequences file, [hdfs_seq.txt or bgl_seq.txt]
labelPath: the input label path [hdfs_label.txt or bgl_label.txt], 1/0 represents anomaly/normal 
max_length: set the max length for a log sequence, depends on the data
vocabulary_size: the log event number
embedding_vector_length: the dimension of a embedding vector
LSTM_unit: how many LSTM unit are used in the bi-LSTM part
rare_output_size: the dimension of output rare event vector
epoch: the epoch number



input:
1. log event sequences (a sequence of log events)
2. corresponding label for each log event sequence

output:
1. predicted log sequence labels, precision, recall, F_measure

key function:
1. loadData() load the dataset
2. splitData() splits the dataset into training data and testing data
3. ourmodel() defines the model structure

Notes:

We use keras framework with tensorflow as backend. Better run with GPU for the training.