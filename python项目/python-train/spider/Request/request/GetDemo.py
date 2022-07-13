'''通过get请求'''
import requests, json

# 基本写法
res = requests.get('http://httpbin.org/get')
print(res.text)

# 超时设置
res = requests.get('http://httpbin.org/get', timeout=1)
print(res.status_code)

from requests.exceptions import ReadTimeout
try:
    res = requests.get('http://httpbin.org/get', timeout=0.5)
except ReadTimeout:
    print('Timeout')

# 带参数的get请求
res = requests.get('http://httpbin.org/get?name=grmey&age=22')
print(res.text)

data = {
    'name': 'germey',
    'age': '22'
}
res = requests.get('http://httpbin.org/get', params=data)
print(res.text)

# 解析json
print('=======================')
print(type(res.text))
print(res.json)
# res.json  本质是调用了下面的方法
print(json.load(res.text))
print(type(res.json))

# 解析二进制数据
res = requests.get('https://github.com/favicon.ico')
print(type(res.text), type(res.content))
print(res.text)
print(res.content)

# 保存二进制文件
with open('favicon.ico', 'wb') as f:
    f.write(res.content)
    f.close()

# 添加headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    'Host': 'httpbin.org'
}
res = requests.get('https://www.zhihu.com/explore', headers=headers)
print(res.text)
