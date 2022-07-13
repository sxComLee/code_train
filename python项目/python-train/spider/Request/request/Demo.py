import requests

res = requests.get('https://www.baidu.com')
print(type(res))
print(res.status_code)
print(type(res.text))
print(res.text)
print(res.cookies)

#各种请求方式
requests.post('http://httpbin.org/post')
requests.put('http://httpbin.org/put')
requests.detlete('http://httpbin.org/detlete')
requests.head('http://httpbin.org/head')
requests.options('http://httpbin.org/options')