import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
# 手写数字数据集，封装好的对象，可以理解为一个字段
digits = datasets.load_digits()
# 可以使用keys()方法来看一下数据集的详情
print(digits.keys())

# 特征的shape
X = digits.data
# (1797, 64)
print(X.shape)
# 标签的shape
y = digits.target
# (1797, )
print(y.shape)
# 标签分类
# array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
print(digits.target_names)
# 去除某一个具体的数据，查看其特征以及标签信息
some_digit = X[666]
# some_digit = array([ 0.,  0.,  5., 15., 14.,  3.,  0.,  0.,  0.,  0., 13., 15.,  9.,15.,  2.,  0.,  0.,  4., 16., 12.,  0., 10.,  6.,  0.,  0.,  8.,16.,  9.,  0.,  8., 10.,  0.,  0.,  7., 15.,  5.,  0., 12., 11.,0.,  0.,  7., 13.,  0.,  5., 16.,  6.,  0.,  0.,  0., 16., 12.,15., 13.,  1.,  0.,  0.,  0.,  6., 16., 12.,  2.,  0.,  0.])
print(some_digit)
print(y[666])
# 也可以这条数据进行可视化
some_digmit_image = some_digit.reshape(8, 8)
plt.imshow(some_digmit_image, cmap = matplotlib.cm.binary)
plt.show()

