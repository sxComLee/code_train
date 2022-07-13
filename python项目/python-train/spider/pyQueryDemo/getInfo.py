from pyquery import PyQuery as pq
doc = pq(filename='../dushu_douban.txt')
items = doc('.nav-items')
print(type(items))
print(items)
'''获取属性'''
print('=================find查找==================')
lis = items.find('.item')
print(type(lis))
print(lis)
print(lis.attr('_class'))


'''获取文本'''
print('=================获取文本==================')
print(lis.text())


'''获取html'''
print('=================获取html==================')
print(lis.html())
