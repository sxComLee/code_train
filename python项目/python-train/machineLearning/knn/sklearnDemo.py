import numpy as np
from sklearn.neighbors import KNeighborsClassifier
# 创建kNN_classifier实例
kNN_classifier = KNeighborsClassifier(n_neighbors=6)

# raw_data_x是特征(x,y 坐标)，raw_data_y是标签，0为良性，1为恶性
raw_data_x = [[3.393533211, 2.331273381], [3.110073483, 1.781539638], [1.343853454, 3.368312451],
              [3.582294121, 4.679917921], [2.280362211, 2.866990212], [7.423436752, 4.685324231],
              [5.745231231, 3.532131321], [9.172112222, 2.511113104], [7.927841231, 3.421455345],
              [7.939831414, 0.791631213]]

raw_data_y = [0, 0, 0, 0, 0, 1, 1, 1, 1,1]
# 设置训练组
X_train = np.array(raw_data_x)
y_train = np.array(raw_data_y)

# kNN_classifier做一遍fit(拟合)的过程，没有返回值，模型就存储在kNN_classifier实例中
fit = kNN_classifier.fit(X_train, y_train)
# KNeighborsClassifier(algorithm='auto', leaf_size=30, metric='minkowski',
#                      metric_params=None, n_jobs=None, n_neighbors=6, p=2,
#                      weights='uniform')
print(fit)
x = [8.90933607318, 3.365731514]

# kNN进行预测predict，需要传入一个矩阵，而不能是一个数组。reshape()成一个二维数组，
#   第一个参数是1表示只有一个数据，第二个参数-1，numpy自动决定第二维度有多少
y_predict = kNN_classifier.predict(x.reshape(1,-1))
print(y_predict)

