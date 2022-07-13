import urllib.request
import socket
import urllib.parse
'''通过urlopen函数进行网页请求，但是这个函数不能进行请求头等参数的传递，更加复杂的请求通过Request函数实现'''
#get类型请求
response = urllib.request.urlopen('http://www.baidu.com')

print(response.read().decode('utf-8'))


#get类型请求
try:
    response = urllib.request.urlopen('http://www.baidu.com',timeout=0.01)
except urllib.error.URLError as e:
    if isinstance(e.reason,socket.timeout):
        print('TIME OUT')

# print(response.read().decode('utf-8'))

#get类型请求
response = urllib.request.urlopen('http://www.baidu.com')

print(type(response))
# response.read()获取响应体内容（字节的形式）的方法，所以需要decode转换成字节的形式
print(response.read().decode('utf8'))
# 状态码和响应头是判断请求是否成功的标志
print(response.status)
print(response.getheaders())
print(response.getheader('Server'))


#get类型请求
response = urllib.request.urlopen('http://www.baidu.com',timeout=1)

print(response.read().decode('utf-8'))


#put类型请求
data = bytes(urllib.parse.urlencode({"word":"hello"}),encoding='utf8')
#http://httpbin.org http测试网址
#有data参数就以post方式发送，没有data参数就以get方式发送
response = urllib.request.urlopen('http://httpbin.org/post',data=data)
print(response.read())
