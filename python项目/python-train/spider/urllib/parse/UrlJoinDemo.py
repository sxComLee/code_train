'''将两个url参数进行合并，前一个参数是默认值，后一个参数根据url组成部分对前一个参数进行覆盖'''
from urllib.parse import urljoin

print(urljoin('http://www.baidu.com','FAQ.html'))
print(urljoin('http://www.baidu.com','https://cuiqiangcai,com/FAQ.html'))
print(urljoin('http://www.baidu.com/about.html','hhttps://cuiqiangcai,com/FAQ.html'))
print(urljoin('http://www.baidu.com/about.html','https://cuiqingcai.com/FAQ.html'))
print(urljoin('http://www.baidu.com?wd=abc','https://cuiqingcai.com/index.hph'))
print(urljoin('www.baidu.com','?category=2#comment'))
print(urljoin('www.baidu.com','?category=2#comment'))
print(urljoin('www.baidu.com','?category=2'))