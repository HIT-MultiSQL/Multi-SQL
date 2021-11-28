import math
import os
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

folder = "/home/iron/expdata"
graphType = "rate"
files = os.listdir(folder)
graphCount = len(files)
columns = int(math.sqrt(graphCount))
currentGraphNum = 0
if columns * columns < graphCount:
    columns = columns + 1
rows = int(graphCount / columns)
if rows * columns < graphCount:
    rows = rows + 1
fig, ax = plt.subplots(rows, columns)
for filename in files:
    x = []
    y = []
    xLabel = ""
    yLabel = ""
    with open(folder + "/" + filename, 'r') as file:
        while True:
            line = file.readline()
            if not line:
                break
            fields = line.split(" ")
            fields[-1] = fields[-1].strip()
            x.append(int(fields[0]))
            xLabel = fields[1]
            y.append(int(fields[2]))
            yLabel = fields[3]
    if graphType == "rate":
        newx = []
        newy = []
        newx.append(x[0] / 2)
        newy.append(y[0] / x[0])
        for i in range(1, len(x)):
            newx.append((x[i] + x[i - 1]) / 2)
            newy.append((y[i] - y[i - 1]) / (x[i] - x[i - 1]))
        x = newx
        y = newy

    ax[currentGraphNum // columns, currentGraphNum % columns].plot(x, y)
    ax[currentGraphNum // columns, currentGraphNum % columns].set_xlabel(xLabel)
    ax[currentGraphNum // columns, currentGraphNum % columns].set_ylabel(yLabel)
    ax[currentGraphNum // columns, currentGraphNum % columns].set_title(filename)
    currentGraphNum = currentGraphNum + 1
fig.tight_layout()
plt.show()

