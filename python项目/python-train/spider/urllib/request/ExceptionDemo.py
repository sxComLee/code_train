from urllib import request,error

try:
    res = request.urlopen('http://cuiqingcai.com/index.html')
except error.URLError as e:
    print(e.reason)

print('=========================')
# 具体验证错误类型的方法
try:
    res = request.urlopen('http://cuiqingcai.com/index.html')
except error.HTTPError as e:
    print(e.reason,e.code,e.headers,sep='\n')
except error.URLError as e:
    print(e.reason)
else:
    print('Request Successfully')

print('=========================')
import socket
try:
    res = request.urlopen('http://cuiqingcai.com/index.html',timeout=0.01)
except error.URLError as e:
    print(type(e.reason))
    if isinstance(e.reason,socket.timeout):
        print('TIME OUT')