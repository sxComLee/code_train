import requests
from requests.auth import HTTPBasicAuth
# 认证设置，账户名，密码
res = requests.get('http://120.27.34.24:9007', auth=HTTPBasicAuth('user', '123'))
res = requests.get('http://120.27.34.24:9007', auth=('user', '123'))
print(res.status_code)
