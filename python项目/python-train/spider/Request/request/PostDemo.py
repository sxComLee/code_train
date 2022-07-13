'''post请求demo'''
import requests

data = {'name': 'germey', 'age': '22'}
res = requests.post('http://httpbin.org/post', data=data)
print(res.text)

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    'Host': 'httpbin.org'
}
res = requests.post('http://httpbin.org/post', data=data,headers=headers)
print(res.text)

