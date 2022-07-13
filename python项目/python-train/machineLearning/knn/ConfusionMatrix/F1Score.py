import numpy as np

def f1_scoer(precision,recall):
    try:
        return 2*precision*recall/(precision+recall)
    except:
        return 0.0

precision = 0.5
recall = 0.5
print('f1 score结果为：',f1_scoer(precision,recall))

precision = 0.9
recall = 0.1
print('f1 score结果为：',f1_scoer(precision,recall))
