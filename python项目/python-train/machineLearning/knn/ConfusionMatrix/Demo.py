'''混淆矩阵实现一个逻辑回归算法'''
import numpy as np
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression

digits = datasets.load_digits()
x = digits.data
y = digits.target.copy()
# 要构造倾斜数据，将数据9对应索引的元素设置为1，0～8设置为0
y[digits.target == 9] = 1
y[digits.target != 9] = 0
# 使用逻辑回归做一个分类
x_train,x_test,y_train,y_test = train_test_split(x,y,random_state=666)

log_reg = LogisticRegression()
log_reg.fit(x_train,y_train)
y_log_predict = log_reg.predict(x_test)
score = log_reg.score(x_test, y_test)
print(score)

'''定义混淆矩阵的四个指标：TN，真实结果是0，预测也为0'''
def TN(y_true , y_predict):
    assert len(y_true) == len(y_predict)
    # (y_true == 0)：向量与数值按位比较，得到的是一个布尔向量
    # 向量与向量按位与，结果还是布尔向量
    # np.sum 计算布尔向量中True的个数(True记为1，False记为0)
    return np.sum((y_true == 0) & (y_predict == 0))
    # 向量与向量按位与，结果还是向量 TN(y_test, y_log_predict)

'''定义混淆矩阵的四个指标：FP,真实结果为0，预测为1'''
def FP(y_true,y_predict):
    assert len(y_true) == len(y_predict)
    # (y_true == 0)：向量与数值按位比较，得到的是一个布尔向量
    # 向量与向量按位与，结果还是布尔向量
    # np.sum 计算布尔向量中True的个数(True记为1，False记为0)
    return np.sum((y_true == 0 )&(y_predict == 1))

'''定义混淆矩阵的四个指标：FN,真实结果为1，预测为0'''
def FN(y_true,y_predict):
    assert len(y_true) == len(y_predict)
    # (y_true == 0)：向量与数值按位比较，得到的是一个布尔向量
    # 向量与向量按位与，结果还是布尔向量
    # np.sum 计算布尔向量中True的个数(True记为1，False记为0)
    return np.sum((y_true == 1 )&(y_predict == 0))

'''定义混淆矩阵的四个指标：FP,真实结果为1，预测为1'''
def TP(y_true,y_predict):
    assert len(y_true) == len(y_predict)
    # (y_true == 0)：向量与数值按位比较，得到的是一个布尔向量
    # 向量与向量按位与，结果还是布尔向量
    # np.sum 计算布尔向量中True的个数(True记为1，False记为0)
    return np.sum((y_true == 1 )&(y_predict == 1))

'''输出混淆矩阵'''
def confusion_matrix(y_true, y_predict):
    return np.array([ [TN(y_true, y_predict),FP(y_true, y_predict)],
                      [FN(y_true, y_predict),TP(y_true, y_predict)]])
# 输出：# array([[403,   2],#       [  9,  36]])
print(confusion_matrix(y_test, y_log_predict))

# 计算准确率
def precision_score(y_true,y_predict):
    tp = TP(y_true,y_predict)
    fp = FP(y_true,y_predict)
    try:
        return tp/(tp+fp)
    except:
        return 0.0

print('准确率为：',precision_score(y_test,y_log_predict))

# 计算召回率
def recall_score(y_true,y_predict):
    tp = TP(y_true,y_predict)
    fn = FN(y_true,y_predict)
    try:
        return tp/(tp+fn)
    except:
        return 0.0
print('召回率',recall_score(y_test,y_log_predict))

from sklearn.metrics import confusion_matrix

matrix = confusion_matrix(y_test, y_log_predict)
print('计算混淆矩阵',matrix)

from sklearn.metrics import precision_score

print('计算精准率',precision_score(y_test, y_log_predict))

from sklearn.metrics import recall_score
print('计算召回率',recall_score(y_test,y_log_predict))

# 计算F1Score
