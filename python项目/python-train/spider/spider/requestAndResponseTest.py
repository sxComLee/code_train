import requests

# response = requests.get('http://www.baidu.com')
headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'}
response = requests.get('http://www.baidu.com')
print('===========获取文件的文本格式===========')
print(response.text)
print('=========获取文件的二进制格式=============')
print(response.content)
print('======================')
print(response.headers)
print('======================')
print(response.status_code)