'''将两个array进行合并'''
import numpy as np

a = np.array([1, 1, 1])
print(a)
# a = np.array([1, 1, 1]).reshape(3,1)
b = np.array([2, 2, 2])
print(a)
# 上下合并,v代表垂直 vertical stack
c = np.vstack((a, b))
# (3,) (2, 3) a的shape因为返回的是一个tuple，如果不写逗号，直接变成了int类型
print(a.shape,c.shape)
print(c)

# 上下合并,h代表水平 horizontal stack
c = np.hstack((a, b))
# (3,) (2, 3) a的shape因为返回的是一个tuple，如果不写逗号，直接变成了int类型
print(a.shape,c.shape)
print(c)


a = np.array([1, 1, 1])[:,np.newaxis]

# a = np.array([1, 1, 1]).reshape(3,1)
b = np.array([2, 2, 2])[:,np.newaxis]
print(a,b)
# 纵向合并
c = np.concatenate((a, b), axis=0)
print(c)
# 横向合并
np.concatenate((a,b),axis=1)
print(c)

