import os, re

dir = os.listdir("/home/ironwei/outlog")
for fileName in dir:
    if fileName.startswith("out"):
        fileName = "/home/ironwei/outlog/" + fileName
        with open(fileName, "r") as f:
            lines = f.readlines()
            for i in range(len(lines)):
                if re.match('^COL\s[0123456789].*', lines[i]) is not None:
                    s = lines[i+2].strip().split(",")
                    print(lines[i].strip(), s[-1].strip(), s[-2].strip())
