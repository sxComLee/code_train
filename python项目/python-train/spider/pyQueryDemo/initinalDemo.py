from pyquery import PyQuery as pq
'''字符串初始化'''
with open('../dushu_douban.txt') as f:
    doc = pq(f.read())
    # 标签名前面什么都不加，id前面加#，class前面加 .
    print(doc('h2'))

'''url初始化'''
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
}
doc = pq(url='http://book.douban.com',headers=headers)
print(doc('head'))

print('============文件初始化========================')
'''文件初始化'''
doc = pq(filename='../dushu_douban.txt')
print(doc('h2 '))

print('============基本的css选择器========================')
'''基本的css选择器'''
doc = pq(filename='../dushu_douban.txt')
# 选择器之间不一定要有直接的层级关系，标签名前面什么都不加，id前面加#，class前面加 .
print(doc('h2 .link-more'))