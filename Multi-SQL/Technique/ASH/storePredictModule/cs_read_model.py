import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from typing import List


class ColumnReadModel:
    def __init__(self):
        self.__model = LinearRegression()
        self.__train()

    def __train(self):
        lines = []
        with open("../storePredictModule/colfc/colfc.log") as f:
            raw = f.readlines()
        for line in raw:
            if line.startswith("Table"):
                lines.append(line)

        features = np.zeros(shape=(len(lines), 5))
        for i in range(len(lines)):
            line = lines[i]
            fields = line.strip().split(' ')
            fieldSize = int(fields[3].split(':')[1])
            rows = int(fields[4].split(':')[1])
            tableSize = fieldSize * rows / (1024 * 1024)
            time = int(fields[5].split(':')[1])
            type = fields[6].split(':')[1]
            if type == 'INT':
                type = 1
            else:
                type = 0
            features[i] = [fieldSize, rows, tableSize, time, type]

        data = pd.DataFrame(data=features, columns=['fieldSize', 'rows', 'tableSize', 'time', 'type'])

        X = data.drop(columns=['time']).copy()
        y = data['time'].copy()
        self.__model.fit(X, y)

    def predict(self, field_size: List[int], rows: int, data_type: List[int]) -> int:
        # orderKey, extendedPrice, shipdate, discount
        X_test = np.zeros(shape=(len(field_size), 4))
        for i in range(len(field_size)):
            X_test[i][0] = field_size[i]
            X_test[i][1] = rows
            X_test[i][2] = field_size[i] * rows / 1024 / 1024
            X_test[i][3] = data_type[i]
        X_test = pd.DataFrame(data=X_test, columns=['fieldSize', 'rows', 'tableSize', 'type'])
        y_pred = self.__model.predict(X=X_test)
        return sum(y_pred)


if __name__ == '__main__':
    model = ColumnReadModel()
    print(model.predict([5, 8], 10000, [1, 1]))