import numpy as np

# 进行矩阵的运算
array = np.array([[1,2,3]
                ,[2,3,4]
                # ,[4,5,6]
                  ])
print(array)
print('number of dim:',array.ndim)
print('shape:',array.shape)
print('size:',array.size)

# numpy创建array
# 一般通过列表的形式进行创建,dtype指定数据的格式
# a = np.array([1, 2, 3],dtype=np.int)
# 位数越小，保存的精度越小，占用内存越小
a = np.array([1, 2, 3],dtype=np.float)
print(a.dtype)

# 定义二维矩阵
a = np.array([[1,2,3],[2,3,4]])
print(a)
print('dim is',a.ndim)
# 定义三维矩阵
a = np.array([[[1,2,3],[2,3,4]]])
print(a)
print('dim is',a.ndim)

# 定义一个全部为0的矩阵
a = np.zeros((3, 4),dtype=np.int16)
print('定义的3行4列全部为0且数据类型为int16的矩阵',a)

# 定义一个全部为1的矩阵
a = np.ones((3, 4),dtype=np.int16)
print('定义的3行4列全部为0且数据类型为int16的矩阵',a)

# 定义一个空的矩阵
a = np.empty((3, 4))
print('定义的3行4列的空矩阵',a)

# 定义一个有序的矩阵
a = np.arange(10,20,2)
print('定义一个大于等于10小于20 且步长为2的矩阵 ',a)

# 定义一个有序的矩阵
a = np.arange(12)
print('生成一个有12位的数列',a)

a = a.reshape((3,4))
print('重新生成一个3行4列的矩阵',a)

a = np.linspace(1, 10, 5)
print('生成一个线段，将1到10分成5段，输出分割点',a)

a = np.linspace(1, 10, 6).reshape((2,3))
print('将线段重新生成数列',a)

