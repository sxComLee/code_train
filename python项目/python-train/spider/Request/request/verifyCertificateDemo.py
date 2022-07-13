'''凭证验证'''
import requests

res = requests.get("https://www.12306.cn")
print(res.status_code)

import urllib3
urllib3.disable_warnings()
res = requests.get("https://www.12306.cn",verify=False)
print(res.status_code)

res = requests.get("https://www.12306.cn",cert=('/path/server.crt','/path/key'))
print(res.status_code)