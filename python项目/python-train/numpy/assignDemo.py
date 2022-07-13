'''为numpy的array进行赋值'''
import numpy as np
# 如果 = 之后，一个发生改变另一个也会变化
a= np.arange(4)
b=a
c=a
d=a
a[0] = 11
print(a)
print(a is b is c is d)

print(b)
# 深拷贝
b = a.copy
a[0] = 0
print(a)
print(b)