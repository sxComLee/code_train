'''会话维持，模拟登陆'''
import requests

res = requests.Session()
res.get('http://httpbin.org/cookies/set/number/123456789')
response = res.get('http://httpbin.org/cookies')
print(response.text)
