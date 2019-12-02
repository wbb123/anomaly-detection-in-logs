import numpy as np
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
from keras.models import Sequential
from keras.models import Model
from keras.layers import Input, Dense, LSTM, Bidirectional, concatenate, Dropout, GaussianNoise
from keras.layers.embeddings import  Embedding
from keras.preprocessing import sequence
import keras.backend as K
from keras import regularizers
from keras import optimizers
import keras
np.random.seed(7)
# thread.daemon = False

trainPercent = 0.8
dataPath = './data/bgl_seq.txt'
labelPath =  './data/bgl_label.txt'
def loadData(dataPath, labelPath):
    data = []
    rareThre = 0.0005
    eventCountList = [0] * 385
    with open(dataPath) as f:
        for row in f:
            rowList = row.split()
            newRow = list(map(int, rowList))
            data.append(newRow)
            uniqueList = set(newRow)
            for ev in range(1, 385):
                if ev in uniqueList:
                    eventCountList[ev - 1] += 1
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
                # row = list(filter(lambda a: a != rareEv, row))
                rareAppearList[i] = 1
        rareEventMat.append(rareAppearList)
        # data[count] = row
        count += 1
    # print(rareEventMat[109984])
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
    y_pred = np.array(y_pred)
    y_true = np.array(y_true)

    count = np.sum(y_pred * y_true)
    pred_failure = y_pred.sum()
    true_failure = y_true.sum()

    print(count, pred_failure, true_failure)
    if count == 0:
        print('precision is 0 and recall is 0')
    else:
        precision = float(count) / (pred_failure)
        print('precision is %.5f' % precision)
        recall = float(count) / (true_failure)
        print('recall is %.5f' % recall)
        F_measure = 2 * precision * recall / (precision + recall)
        print('F_measure is %.5f' % F_measure)

def ourmodel(x_train, x_test, y_train, y_test, x_train_rare, x_test_rare):
    K.set_learning_phase(0)
    print([len(l) for l in x_train])
    max_length = 70
    print(max_length)
    x_train = sequence.pad_sequences(x_train, maxlen=max_length, padding='post', truncating='post')
    x_test = sequence.pad_sequences(x_test, maxlen=max_length, padding='post', truncating='post')

    embedding_vector_length = 16       #vector size
    rareNum = x_train_rare.shape[1]
    vocabulary_size = 385
    LSTM_unit = 16
    rare_output_size = 30
    # LSTM part
    lstm_input = Input(shape=(max_length,), name='main_input')  # after padding length
    x = Embedding(vocabulary_size, embedding_vector_length, mask_zero=True, input_length=max_length)(lstm_input)
    biLSTM_output = Bidirectional(LSTM(LSTM_unit, dropout=0.3, recurrent_dropout=0.2, return_sequences=False), merge_mode='sum')(x) #
    denseOutput = Dense(rare_output_size, activation='relu')(biLSTM_output)
    # Rare event part
    rare_input = Input(shape=(rareNum,), name='aux_input')
    merged = concatenate([denseOutput, rare_input])
    prediction = Dense(1, activation='sigmoid')(merged)
    model = Model(inputs=[lstm_input, rare_input], outputs=prediction)

    adam = optimizers.Adam(lr=0.0008, beta_1=0.9, beta_2=0.999, epsilon=1e-08, decay=0.0)
    model.compile(loss='binary_crossentropy', optimizer=adam, metrics=[F_measure, precision, recall, 'accuracy'])
    print(model.summary())
    rec_ten_board = keras.callbacks.TensorBoard(log_dir='./logs', histogram_freq=1, write_graph=True, write_images=False)
    earlystop = keras.callbacks.EarlyStopping(monitor='val_F_measure', min_delta=0, patience=8, verbose=0, mode='auto')
    model.fit([x_train, x_train_rare], y_train, epochs=50, shuffle=True, batch_size=1024, validation_data=([x_test, x_test_rare], y_test),callbacks = [rec_ten_board, earlystop])
    scores = model.evaluate([x_test, x_test_rare], y_test, batch_size=1024, verbose=0)

    print("F_Measure: %.2f%%, precision: %.2f%%, recall: %.2f%%,  accuracy: %.2f%%, " % (
    scores[1] * 100, scores[2] * 100, scores[3] * 100, scores[4] * 100))
    prediction = model.predict([x_test, x_test_rare], batch_size=256, verbose=0)

    rounded_predict = np.array([round(x[0]) for x in prediction])
    print('the testing data size is %d' % (len(y_test)))
    print('the prediction data size is %d' % (len(rounded_predict)))
    test_precision(y_test, rounded_predict)

def precision(y_true, y_pred):
    round_y_pred = K.round(y_pred)
    return K.sum(y_true * round_y_pred) / K.sum(round_y_pred)

def recall(y_true, y_pred):
    round_y_pred = K.round(y_pred)
    return K.sum(y_true * round_y_pred) / K.sum(y_true)

def F_measure(y_true, y_pred):
    round_y_pred = K.round(y_pred)
    pre = precision(y_true, round_y_pred)
    rec = recall(y_true, round_y_pred)
    return 2 * pre * rec/(pre+rec)


data, labelData, rareEventMat = loadData(dataPath, labelPath)
x_train, x_test, y_train, y_test, x_train_rare, x_test_rare = splitData(trainPercent, data, labelData, rareEventMat)
ourmodel(x_train, x_test, y_train, y_test, x_train_rare, x_test_rare)

