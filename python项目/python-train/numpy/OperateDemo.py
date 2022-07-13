import numpy as np

a = np.array([10, 20, 30, 40])
b = np.arange(4)

print(a, b)
c = a + b
print(c)

c = a - b
print(c)

c = a * b
print(c)

#  c为a的平方
c = a ** 2
print(c)

# 求三角函数
c = np.sin(a)
c = np.cos(a)
c = np.tan(a)

# 选择关系
print(b)
print(b < 3)
print(b == 3)

# 计算矩阵
a = np.array([[1,1]
              ,[0,1]])

b = np.arange(4).reshape(2,2)
print('分别输出a，b：',a,b)
# 逐个相乘
c = a*b
# 矩阵相乘
c_dot = np.dot(a,b)
c_dot_2 = a.dot(b)
print('矩阵各个相乘结果',c)
print('矩阵相乘结果：',c_dot)
print('矩阵相乘结果：',c_dot_2)


# 求聚合结果
a = np.random.random((2,4))
print(a)
print(np.sum(a))
# axis=1在列数当中的求和，axis=0在行数当中的求和，不写默认是全部的
print(np.sum(a,axis=1))
print(np.sum(a,axis=0))
print(np.max(a))
print(np.max(a,axis=1))
print(np.max(a,axis=0))
print(np.min(a))
print(np.min(a,axis=1))
print(np.min(a,axis=0))
print(np.mean(a,axis=1))
print(np.mean(a,axis=0))

#
a = np.arange(2,14).reshape((3,4))
print(a)
# 计算最小值的索引
print('矩阵最小值索引',np.argmin(a))
print('矩阵最小值索引',a.argmin())
print('矩阵最大值索引',np.argmax(a))
print('矩阵最大值索引',a.argmax())
print('矩阵平均值',np.mean(a))
print('矩阵平均值',a.mean())
print('矩阵平均值',np.average(a))
# print('矩阵平均值',a.average())
# 不支持以下算法
print('矩阵中位数',np.median(a))
# 不支持以下算法
# print('矩阵中位数',a.median())

print('将矩阵中的数据进行累加',np.cumsum(a))
print('将矩阵中的数据进行累差',np.diff(a))
# 输出两个array，第一个array对应的是行索引，第二个array对应的是列索引
print('将矩阵中的非0的数进行输出',np.nonzero(a))
print('将行列翻转 / 转置',np.transpose(a))
print('将行列翻转 / 转置',a.T)
print('将行列翻转 / 转置后计算',(a.T).dot(a))

print('将array中小于min都是min，所有大于max都是max，中间不变',np.clip(a,5,9))

