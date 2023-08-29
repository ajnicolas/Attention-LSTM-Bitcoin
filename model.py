from database import SqlDatabase
import talib
import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, LSTM, Dense, Flatten, Multiply, RepeatVector, Permute, Dropout
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.losses import binary_crossentropy


test = SqlDatabase("XBTUSD", "1d", False)
print(test.get_first())
print(test.get_last())


close_data = test.ohlcv_range('close', "2016-09-26", "2023-05-20")

# Preparing data
data = close_data['close'].values.reshape(-1, 1)

scaler = MinMaxScaler(feature_range=(0, 1))
scaled_data = scaler.fit_transform(data)

# split data
train_size = int(len(scaled_data) * 0.8)
train_data = scaled_data[:train_size]
test_data = scaled_data[train_size:]

# Create sequences for training and test using sliding window
def create_sequences(data, seq_length):
    X, y = [], []
    for i in range(len(data) - seq_length):
        X.append(data[i:i + seq_length])
        y.append(data[i + seq_length])
    return np.array(X), np.array(y)

seq_length = 26
X_train, y_train = create_sequences(train_data, seq_length)
X_test, y_test = create_sequences(test_data, seq_length)

lstmUnits = 500

inputs = Input(shape=(seq_length, 1))

# LSTM layers
lstm = LSTM(units=lstmUnits, return_sequences=True)(inputs)

lstm = LSTM(units=lstmUnits, return_sequences=True)(lstm)

lstm = LSTM(units=lstmUnits)(lstm)
lstm = Dropout(0.3)(lstm)


# Attention mechanism
attention = Dense(1, activation='tanh')(lstm)
attention = Flatten()(attention)
attention = RepeatVector(lstmUnits)(attention)
attention = Permute([2, 1])(attention)

# LSTM layer with attention
lstm_attention = Multiply()([lstm, attention])
lstm_attention = LSTM(units=lstmUnits)(lstm_attention)
lstm_attention = Dropout(0.3)(lstm_attention)

output = Dense(units=1, activation='sigmoid')(lstm_attention)

model = Model(inputs=inputs, outputs=output)
model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
model.fit(X_train, y_train, epochs=100, batch_size=32)

# Evaluate the model
predictions = model.predict(X_test)
predictions = scaler.inverse_transform(predictions)
y_test = scaler.inverse_transform(y_test)

for i in range(len(predictions)):
    sequence = X_test[i].reshape(-1, 1)
    sequence = scaler.inverse_transform(sequence)
    print("Sequence:", sequence)
    print("Prediction:", predictions[i])
    print("y_test:", y_test[i])
    print()
loss, accuracy = model.evaluate(X_test, y_test)
print("Test loss:", loss)
print("Test accuracy:", accuracy)

