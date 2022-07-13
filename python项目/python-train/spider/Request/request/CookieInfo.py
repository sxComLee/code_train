'''cookie详细信息'''
import requests

res = requests.get('http://www.baidu.com')
print(res.cookies)
for key,value in res.cookies.items():
    print(key + '=' + value)
