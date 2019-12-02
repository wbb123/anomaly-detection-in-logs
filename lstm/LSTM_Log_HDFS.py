import numpy as np
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
from keras.models import Sequential
from keras.layers import Input, Dense, LSTM, Bidirectional, concatenate, Dropout
from keras.layers.embeddings import  Embedding
from keras.preprocessing import sequence
import keras.backend as K
from keras import optimizers
from keras.models import Model
import keras
np.random.seed(7)

trainPercent = 0.8
dataPath = './data/hdfs_seq.txt'
labelPath =  './data/hdfs_label.txt'

def loadData(dataPath, labelPath):
    data = []
    rareThre = 0.0005
    eventCountList = [0] * 29
    with open(dataPath) as f:
        for row in f:
            rowList = row.split()
            newRow = list(map(int, rowList))
            data.append(newRow)
            uniqueList = set(newRow)
            for ev in range(1,30):
                if ev in uniqueList:
                    eventCountList[ev-1]+=1
    print(eventCountList)
    minCount = len(data) * rareThre
    rareEvent = []
    rarecount = 1
    for evCount in eventCountList:
        if evCount <= minCount:
            rareEvent.append(rarecount)
        rarecount += 1
    print(rareEvent)
    count = 0
    rareEventMat = []
    rareLen = len(rareEvent)
    for row in data:
        rareAppearList = [0] * rareLen
        for i in range(rareLen):
            rareEv = rareEvent[i]
            if rareEv in row:
                rareAppearList[i] = 1
        rareEventMat.append(rareAppearList)
        count+=1
    labelData = []
    with open(labelPath) as lf:
        for larow in lf:
            larowList = larow.split()
            labelData.append(int(larowList[0]))
        labelData = np.array(labelData)
    data = np.array(data)
    rareEventMat = np.array(rareEventMat)
    return data, labelData, rareEventMat

def splitData(trainPercent, data, labelData, rareEventMat):
    dataSize = data.shape[0]
    trainSize = int(dataSize * trainPercent)
    print('training size is ', trainSize)
    numlist = np.arange(dataSize)
    np.random.shuffle(numlist)
    newdata = np.array([data[sl] for sl in numlist])
    newlabel = np.array([labelData[sl] for sl in numlist])
    newrare = np.array([rareEventMat[sl] for sl in numlist])
    x_train = newdata[:trainSize]
    x_train_rare = newrare[:trainSize]
    x_test = newdata[trainSize:dataSize]
    x_test_rare = newrare[trainSize:dataSize]
    y_train = newlabel[:trainSize]
    y_test = newlabel[trainSize:dataSize]

    np.testing.assert_equal(x_train.shape[0]+x_test.shape[0], dataSize)
    np.testing.assert_equal(y_train.shape[0] + y_test.shape[0], dataSize)
    print('x_train_rare size is ', x_train_rare.shape[1])
    return x_train, x_test, y_train, y_test, x_train_rare, x_test_rare

def test_precision(y_true, y_pred):
    np.testing.assert_equal(len(y_pred), len(y_true))
    commonfail = 0
    for i in range(len(y_pred)):
        if (y_true[i] == 1):
            if (y_pred[i] == 1):
                commonfail += 1
    pred_failure = y_pred.sum()
    true_failure = y_true.sum()
    print(commonfail, pred_failure, true_failure)
    if commonfail == 0:
        print('precision is 0 and recall is 0')
    else:
        precision = float(commonfail) / (pred_failure)
        print('precision is %.5f' % precision)
        recall = float(commonfail) / (true_failure)
        print('recall is %.5f' % recall)
        F_measure = 2 * precision * recall / (precision + recall)
        print('F_measure is %.5f' % F_measure)

def ourmodel(x_train, x_test, y_train, y_test, x_train_rare, x_test_rare):
    K.set_learning_phase(0)
    max_length = 45  # by counting all the traning data length, 45 is suitable, and we will truncate remaining values
    x_train = sequence.pad_sequences(x_train, maxlen=max_length, padding='pre', truncating='pre')
    x_test = sequence.pad_sequences(x_test, maxlen=max_length, padding='pre', truncating='pre')

    print('x_train_rare size is ', x_train_rare.shape[1])
    vocabulary_size = 29
    embedding_vector_length = 20
    rareNum = x_train_rare.shape[1]
    LSTM_unit = 32
    rare_output_size = 20

    #LSTM part
    lstm_input = Input(shape=(max_length, ), name='main_input')  # after padding length
    x = Embedding(vocabulary_size, embedding_vector_length, mask_zero=True, input_length=max_length)(lstm_input)
    biLSTM_output = Bidirectional(LSTM(LSTM_unit, return_sequences= False, dropout=0.2, recurrent_dropout=0.2), merge_mode='sum')(x) # return_sequences= False,  dropout=0.3, recurrent_dropout=0.3
    denseOutput = Dense(rare_output_size, activation='relu')(biLSTM_output)

    #Rare event part
    rare_input = Input(shape=(rareNum,), name='aux_input')
    merged = concatenate([denseOutput, rare_input])
    prediction = Dense(1, activation='sigmoid')(merged)
    model = Model(inputs= [lstm_input, rare_input], outputs= prediction)

    adam = optimizers.Adam(lr=0.001, beta_1=0.9, beta_2=0.999, epsilon=1e-08, decay=0.0)
    model.compile(loss='binary_crossentropy', optimizer=adam, metrics=[F_measure, precision, recall,  'accuracy'])
    print(model.summary())
    rec_ten_board = keras.callbacks.TensorBoard(log_dir='./logs', histogram_freq=1, write_graph=True, write_images=False)
    earlystop =keras.callbacks.EarlyStopping(monitor='val_loss', min_delta=0, patience=5, verbose=0, mode='auto')

    model.fit([x_train, x_train_rare], y_train, epochs=50, shuffle=True, batch_size=2048, validation_data=([x_test, x_test_rare], y_test), callbacks = [earlystop])
    scores = model.evaluate([x_test, x_test_rare], y_test, batch_size= 2048, verbose=0)
    print("F_Measure: %.2f%%, precision: %.2f%%, recall: %.2f%%,  accuracy: %.2f%%, " % (scores[1] * 100, scores[2] * 100, scores[3] * 100, scores[4] * 100))
    prediction = model.predict([x_test, x_test_rare], batch_size=1024, verbose=0)

    rounded_predict = np.array([round(x[0]) for x in prediction])
    print('the testing data size is %d' % (len(y_test)))
    print('the prediction data size is %d' % (len(rounded_predict)))
    test_precision(y_test, rounded_predict)

def precision(y_true, y_pred):
    return K.sum(y_true * y_pred) / K.sum(y_pred)

def recall(y_true, y_pred):
    return K.sum(y_true * y_pred) / K.sum(y_true)

def F_measure(y_true, y_pred):
    pre = precision(y_true, y_pred)
    rec = recall(y_true, y_pred)
    return 2 * pre * rec/(pre+rec)

data, labelData, rareEventMat = loadData(dataPath, labelPath)
x_train, x_test, y_train, y_test, x_train_rare, x_test_rare = splitData(trainPercent, data, labelData, rareEventMat)
ourmodel(x_train, x_test, y_train, y_test, x_train_rare, x_test_rare)
