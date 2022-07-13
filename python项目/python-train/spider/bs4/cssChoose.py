'''css选择器，通过select()直接传入css选择器就可以完成选择'''
from bs4 import BeautifulSoup

with open('../dushu_douban.txt') as f:
    soup = BeautifulSoup(f.read(), 'lxml')
    # 如果选择器为class的话，那么内容前面需要加一个 . 选择class为global-nav-items 下的class为on的标签
    print(soup.select('.global-nav-items .on'))
    # 选择标签的话，直接写就行
    # print(soup.select('div a'))
    # 选择id的话，需要在值之前加一个 #
    print(soup.select('#doubanapp-tip .tip-link'))

    # 通过遍历的方式查找
    # for div in soup.select('div'):
        # print(div.select('.global-nav-items'))

    # 获取属性
    for div in soup.select('#doubanapp-tip .tip-link'):
        print(div['href'])
        print(div.attrs['href'])
        # 获取内容
        print(div.get_text())
