'''标准选择器，可根据标签名，属性，内容查找文档
   find_all(name,attrs,recursive,text,**kwargs)'''
from bs4 import BeautifulSoup
with open('../dushu_douban.txt') as f:
    soup = BeautifulSoup(f.read(),'lxml')
    '''find_all方法，返回所有的匹配的元素'''
    print(soup.find_all('div'))
    print(type(soup.find_all('div')[0]))
    for div in soup.find_all('div'):
        print(div.find_all('a'))

    print("==========根据属性进行查找==============")
    print(soup.find_all(attrs={'class':'icon-embassy-site'}))
    # 因为class是关键字，所以在用class属性的时候，加上下划线
    print(soup.find_all(class_='icon-embassy-site'))

    print("==========根据属性进行查找==============")
    # 适合做内容匹配而不是元素查找
    print(soup.find_all(text='微信'))

    '''find方法，返回第一个的匹配的元素'''
    print(soup.find('div'))
    print(type(soup.find('div')))
    # 查找不存在的元素返回None
    print(soup.find('source'))