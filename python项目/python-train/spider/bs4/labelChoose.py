'''标签选择器，根据标签对内容进行选择'''
from bs4 import BeautifulSoup

with open('../dushu_douban.txt') as f:
    soup = BeautifulSoup(f.read(), 'lxml');
    # 格式化代码，对代码进行补全处理
    print(soup.prettify())
    # 将html代码的title打印出来
    print(soup.title.string)
    print('=============title====================')
    # 选择元素，标签选择器，如果只有一个就返回它，如果有多个，那么返回匹配的第一个内容
    #  打印title标签，如下
    # <title>
    #     豆瓣读书
    # </title>
    print(soup.title)
    # 打印title标签内容，豆瓣读书
    print(soup.title.string)
    # class 'bs4.element.Tag'>
    print(type(soup.title))
    # 返回标签名称
    print(soup.title.name)
    # 获取标签子节点
    print(soup.title.contents)
    # list类型
    print(type(soup.title.contents))
    # 获取标签子节点，得到的结果是一个迭代器，通过循环遍历
    print(soup.title.child)
    for i,child in enumerate(soup.title.child):
        print(i,child)

    # 获取标签所有的子孙节点，得到的结果是一个迭代器，通过循环遍历，孙节点和字节点同时显示
    print(soup.title.descendants)
    for i,child in enumerate(soup.title.descendants):
        print(i,child)

    # 获取父节点
    print(soup.title.parent)
    # 获取所有的祖先节点
    print(soup.title.parents)
    print(list(enumerate(soup.title.parents)))

    # 获取兄弟节点
        # 获取后面的兄弟节点
    print(list(enumerate(soup.title.next_siblings)))
        # 获取前面的兄弟节点
    print(list(enumerate(soup.title.previous_siblings)))

    print('============head=====================')
    # head 返回第一个head标签内容
    print(soup.head)
    # 输出内容
    print(soup.head.string)
    print(soup.head.name)
    print(type(soup.head))
    print(soup.head.contents)
    print(type(soup.head.contents))
    print('=============p====================')
    # p 返回了第一个P标签内容
    print(soup.p)
    print(soup.p.name)
    print(type(soup.p))
    # 获取属性
    print(soup.p.attrs['class'])
    print(soup.p['class'])
    print('=================================')
