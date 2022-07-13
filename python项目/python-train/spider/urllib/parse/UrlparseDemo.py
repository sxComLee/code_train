'''解析拆分url'''
from urllib.parse import urlparse

result = urlparse('http://www.baidu.com.index.html;user?id=5#comment')
print(result,type(result),sep='\n')

#schema参数，如果链接有协议类型，那么不生效，即指定的是默认的协议
# ParseResult(scheme='https', netloc='', path='www.baidu.com.index.html', params='user', query='id=5', fragment='comment')
result = urlparse('www.baidu.com.index.html;user?id=5#comment',scheme='https')
print(result,type(result),sep='\n')
# ParseResult(scheme='http', netloc='www.baidu.com.index.html;user', path='', params='', query='id=5', fragment='comment')
result = urlparse('http://www.baidu.com.index.html;user?id=5#comment',scheme='https')
print(result,type(result),sep='\n')

# 是否允许锚点信息
# ParseResult(scheme='', netloc='', path='www.baidu.com.index.html', params='user', query='id=5#comment', fragment='')
result = urlparse('www.baidu.com.index.html;user?id=5#comment',allow_fragments=False)
print(result)

# ParseResult(scheme='', netloc='', path='www.baidu.com.index.html#comment', params='', query='', fragment='')
result = urlparse('www.baidu.com.index.html#comment',allow_fragments=False)
print(result)