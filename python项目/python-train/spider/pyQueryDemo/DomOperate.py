from pyquery import PyQuery as pq

doc = pq(filename='../dushu_douban_demo.txt',charset='GBK')

'''addClass,removeClass操作'''
a = doc('.item-1.active')
print(a)
a.remove_class('active')
print(a)
a.add_class('active')
print(a)

'''attr,css修改属性，有则增加，无则修改'''
a.attr('name','li')
print(a)
a.css('font-size','14px')
print(a)

'''remove操作'''
html='''
<div class='wrap'>
    Hello world
    <p>This is a paragraph</p>
</div>
'''
doc = pq(html)
wrap = doc('.wrap')
print(wrap.text())
wrap.find('p').remove()
print(wrap.text())

'''其他DOM操作，http://pyquery.readthedocs.io/en/latest/api.html'''
