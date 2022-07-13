'''上传文件'''
import requests

files = {
    'file':open('Demo.py','rb')
}
res = requests.post('http://httpbin.org/post', files=files)
print(res.text)
