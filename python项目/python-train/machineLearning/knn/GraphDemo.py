import numpy as np
import matplotlib.pyplot as plt
# knn的优点就是分类精确，缺点就是计算量大，尤其多维数据，耗时，一般knn处理不了高维数据，实在想处理的话，就得做降维处理
# 适用于对数据假设性要求不高的，想要做多分类的
# raw_data_x是特征(x,y 坐标)，raw_data_y是标签，0为良性，1为恶性
raw_data_x = [[3.393533211, 2.331273381], [3.110073483, 1.781539638], [1.343853454, 3.368312451],
              [3.582294121, 4.679917921], [2.280362211, 2.866990212], [7.423436752, 4.685324231],
              [5.745231231, 3.532131321], [9.172112222, 2.511113104], [7.927841231, 3.421455345],
              [7.939831414, 0.791631213]]

raw_data_y = [0, 0, 0, 0, 0, 1, 1, 1, 1,1]
# 设置训练组
X_train = np.array(raw_data_x)
y_train = np.array(raw_data_y)

# 将数据可视化
# X_train数据以及y_train数据已经列出，记录下最后两行代码的意思：
# scatter（x,y,color）x就是X轴坐标，Y就是Y轴坐标，color表示颜色，其中X_train[y_train==0,0]的意思就是将
#   X_train（二维数组）中第N行第0个元素取出，而这里的N是多少呢？就是x参数为TRUE时，y_train向量的索引，第二行代码同理
# X_train[y_train==0,0]是个二维数组，数组的第一个参数表示行，这里面是一个布尔表达式即选取y_train值为0的那一行；第二个参数为0，表示第0列
plt.scatter(X_train[y_train==0,0],X_train[y_train==0,1],color='g',label = 'Tumor Size')
plt.scatter(X_train[y_train==1,0],X_train[y_train==1,1],color='r',label = 'Time')

# 设定x轴，y轴
plt.xlabel('Tumor Size')
plt.ylabel('Time')
# 限制横纵坐标的最大值 x轴距离范围为 0～10 y轴距离范围为 0～5
plt.axis([0,10,0,5])
# plt.show()

print("================= 计算距离  ================")
from math import sqrt
distances = [] # 用来记录x到样本数据集中每个点的距离
x = [8.90933607318, 3.365731514]
# for x_train in X_train:
# 	d = sqrt(np.sum((x_train - x) ** 2))
# 	distances.append(d)
 # 使用列表生成器，一行就能搞定，对于X_train中的每一个元素x_train都进行前面的运算，把结果生成一个列表
#  采用的距离是欧式距离，假设一个直角三角形 abc，斜边ac，那么ac就是欧式距离，ab+bc就是曼哈顿距离
# 连续数据用欧氏距离，稀疏数据就采用余弦系数计算方式
distances = [sqrt(np.sum((x_train -x ) ** 2)) for x_train in X_train]
print(distances)


print("================= 计算最小距离  ================")
nearest = np.argsort(distances)
# 结果的含义是：距离最小的点在distances数组中的索引是7，第二小的点索引是8... 近到远是哪些点
# 输出结果 [7 8 5 9 6 3 0 1 4 2]
print(nearest)

print("================= 选择k值 ================")
k = 6
topK_y = [y_train[i] for i in nearest[:k]]
# 那就找出最近的6个点（top 6），并记录他们的标签值（y）
print(topK_y)

print("================= 投票环节 ================")
from collections import Counter

votes = Counter(topK_y)
# 输出：一个字典，原数组中值为0的个数为1，值为1的个数为5
# Counter({0:1, 1:5})
print(votes)

# Counter.most_common(n) 找出票数最多的n个元素，返回的是一个列表，列表中的每个元素是一个元组，
#   元组中第一个元素是对应的元素是谁，第二个元素是频次
# 输出：[(1,5)]
common = votes.most_common(1)
print('出现频次最高的元祖为：',common)

predict_y = votes.most_common(1)[0][0]
# 输出：1
print("最终对于标签的预测结果为：",predict_y)

print("================= 通过自己封装的KNN算法进行结果计算 ================")

from machineLearning.knn.KNNClassifier_self import kNNClassifier_self

knn_clf = kNNClassifier_self(k=6)
knn_clf.fit(X_train,y_train)
X_predict = x.reshape(1,-1)
y_predict = knn_clf.predict(X_predict)
print("通过自己封装的到结果为：",y_predict)


