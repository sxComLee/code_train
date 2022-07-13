import requests

res = requests.get('http://www.jianshu.com')
exit() if not res.status_code == 200 else print('Request Successfully')
