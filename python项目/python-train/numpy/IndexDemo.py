'''在numpy array中根据位置找值，根据位置信息处理运算'''
import numpy as np

a = np.arange(3,15)
print(a)
print(a[2])

a = np.arange(3,15).reshape((3,4))
print(a)
# 选取第3行的数据
print(a[2])
# 选出第一行第一列的数据
print(a[1][1])
print(a[1,1])
# 第2行的所有数
print(a[1:])
# 第1列的所有数
print(a[:,1])

# 第1行的从第一列到第三列
print(a[1,0:2])

# 默认的迭代是迭代行
print('迭代行')
for row in a:
    print(row)

# 迭代列
print('迭代列')
for col in a.T:
    print(col)

# 迭代每一项
print('迭代每一项')
# for col in a.flat():
for col in a.flatten():
    print(col)