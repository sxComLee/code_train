import numpy as np
import matplotlib.pyplot as plt
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression

digits = datasets.load_digits()
x = digits.data
y = digits.targts.copy()

# 要构造偏斜数据，将数字9的对应索引的元素设置为1，0～8设置为0
y[digits.target==9]=1
y[digits.target!=9]=0

# 使用逻辑回归做一个分类
X_train, X_test, y_train, y_test = train_test_split(x, y, random_state=666)

log_reg = LogisticRegression()
log_reg.fit(X_train,y_train)
# 计算逻辑回归给予X_test样本的决策数据值
# 通过decision_function可以调整精准率和召回率
decision_scores = log_reg.decision_function(X_test)

# 计算TP
def TP(y_true,y_predict):
    assert len(y_true) == len(y_predict)
    return np.sum((y_true ==1)&(y_predict==1))
# 计算FN
def FN(y_true,y_predict):
    assert len(y_true) == len(y_predict)
    return np.sum((y_true == 1)&(y_predict==0))
# 计算FP
def FP(y_true,y_predict):
    assert len(y_true) == len(y_predict)
    return np.sum((y_true == 0)&(y_predict ==1))
# 计算TN
def TN(y_true,y_predict):
    assert len(y_true) == len(y_predict)
    return np.sum((y_true == 0)&(y_predict ==0))

# TPR TPR就是所有正例中，有多少被正确地判定为正
def TPR(y_true,y_predict):
    tp = TP(y_true,y_predict)
    fn = FN(y_true,y_predict)
    try:
        return tp / (tp+fn)
    except:
        0.0

#FPR FPR就是所有负例中，有多少被错误地判定为正
def FPR(y_true,y_predict):
    fp = FP(y_true,y_predict)
    tn = TN(y_true,y_predict)
    try:
        return fp / (fp+tn)
    except:
        0.0

fprs = []
tprs = []
# 以0.1为步长，遍历decision_scores中的最小值到最大值的所有数据点，将其作为阈值集合
thresholds = np.arange(np.min(decision_scores),np.max(decision_scores),0.1)
for threshold in thresholds:
    # decision_scores >= threshold 是布尔型向量，用dtype设置为int
    # 大于等于阈值threshold分类为1，小于为0，用这种方法得到预测值
    y_predict = np.array(decision_scores >= threshold,dtype=int)
    #print(y_predict)
    # print(y_test)
    #print(FPR(y_test, y_predict))
    # 对于每个阈值，所得到的FPR和TPR都添加到相应的队列中
    fprs.append(FPR(y_test, y_predict))
    tprs.append(TPR(y_test, y_predict))

# 绘制ROC曲线，x轴是fpr的值，y轴是tpr的值
plt.plot(fprs, tprs)
plt.show()

from sklearn.metrics import roc_curve
fprs, tprs, thresholds = roc_curve(y_test, decision_scores)
plt.plot(fprs, tprs)
plt.show()