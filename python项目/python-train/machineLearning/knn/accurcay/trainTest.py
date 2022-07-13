import numpy as np
from sklearn import datasets
import matplotlib.pyplot as plt
# 获取测试数据
iris = datasets.load_iris()
x = iris.data
y = iris.target

# (150,4)
print(x.shape)
# print(x)
# (150,)
print(y.shape)
# print(y)

#shuffle操作后的数据 训练数据：测试数据 = 8：2
# 将X和y合并为同一个矩阵，然后对矩阵进行shuffle，之后再分解

# 第一种方法使用concatenate函数进行拼接，因为传入的矩阵必须具有相同的形状。
#   因此需要对label进行reshape操作，reshape(-1,1)表示行数自动计算，1列。axis=1表示纵向拼接。
temConcat = np.concatenate((x,y.reshape(-1,1)),axis=1)
# 拼接好后，直接进行乱序操作
np.random.shuffle(temConcat)
# 再将shuffle后的数组使用split方法拆分
shuffle_x,shuffle_y = np.split(temConcat, [4], axis=1)
# print(shuffle_x,shuffle_y)
# 设置划分的比例
test_ratio = 0.2
test_size = int(len(x) * test_ratio)
x_train = shuffle_x[test_size:]
y_train = shuffle_y[test_size:]
x_test = shuffle_x[:test_size]
y_test = shuffle_y[:test_size]

print(x_train.shape)
print(x_test.shape)
print(y_train.shape)
print(y_test.shape)

'''第二种方法 '''
'''区别在于shuffle直接在原来的数组上进行操作，改变原来数组的顺序，无返回值。而permutation不直接在原来的数组上进行操作，而是返回一个新的打乱顺序的数组，并不改变原来的数组。'''
# 将x长度这么多的数，返回一个新的打乱顺序的数组，注意，数组中的元素不是原来的数据，而是混乱的索引
shuffle_index = np.random.permutation(len(x))
# 指定测试数据的比例
test_ratio = 0.2
test_size = int(len(x) * test_ratio)
test_index = shuffle_index[:test_size]
train_index = shuffle_index[test_size:]
x_train = x[train_index]
x_test = x[test_index]

y_train = y[train_index]
y_test = y[test_index]
# 输出
print(x_train.shape)
print(x_test.shape)
print(y_train.shape)
print(y_test.shape)

# 通过方法调用
from machineLearning.knn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(x, y)
print(X_train.shape)
print(X_test.shape)
print(y_train.shape)
print(y_test.shape)

from machineLearning.knn.KNNClassifier_self import kNNClassifier_self

my_kNNClassifier = kNNClassifier_self(k=3)
my_kNNClassifier.fit(X_train, y_train)

y_predict = my_kNNClassifier.predict(X_test)
print('y_predict',y_predict)
print('y_test',y_test)
print(y_test==y_predict)
print(sum(y_test==y_predict))
print(sum(y_test==y_predict)/len(y_test))
# 两个向量的比较，返回一个布尔型向量，对这个布尔向量（faluse=1，true=0）sum，
#   sum(y_predict == y_test)29
#   sum(y_predict == y_test)/len(y_test) 0.96666666666667

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=666)
print(X_train.shape)
print(X_test.shape)
print(y_train.shape)
print(y_test.shape)

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.neighbors import KNeighborsClassifier

X_train, X_test, y_train, y_test  = train_test_split(x, y, test_size=0.2, random_state=666)
knn_clf = KNeighborsClassifier(n_neighbors=3)
knn_clf.fit(X_train, y_train)
y_predict = knn_clf.predict(X_test)
score = accuracy_score(y_test, y_predict)
print(score)