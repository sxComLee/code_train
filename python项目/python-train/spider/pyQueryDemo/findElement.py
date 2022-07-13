'''查找元素'''
from pyquery import PyQuery as pq

doc = pq(filename='../dushu_douban.txt')
items = doc('.list')
print(type(items))
# print(items)
'''查找子元素，find方法，会层层迭代，向下向里查找'''
print('=================find查找==================')
lis = items.find('.item .rank-num')
print(type(lis))
print(lis)
print('=================children查找==================')
'''查找子元素，children方法只会查找直接子元素'''
lis = items.children('.rank-num')
print(type(lis))
# 输出为空
print(lis)

lis = items.children('.item .rank-num')
print(type(lis))
print(lis)

print('=================parent查找父元素==================')
'''查找父元素'''
lis = items.parent()
print(type(lis))
# print(lis)

'''查找祖先元素'''
lis = items.parents()
print(type(lis))
# print(lis)

print('=================sublings查找兄弟元素==================')
# 第一个选择器的class为list，第二个选择器的class既包含item-0，也包含active
li = doc('.list .item-0.active')
# 也可以传入选择器进行二次筛选
print(li.siblings())

print('=================遍历元素==================')
lis = items.children('.item .rank-num').items()
print(type(lis))
for li in lis:
    print(li)

