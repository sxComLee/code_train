import requests
from requests.exceptions import *

try:
    res = requests.get('http://httpbin.org/get', timeout=0.01)
    print(res.status_code)
except ReadTimeout:
    print('TimeOut')
except ReadTimeout:
    print('TimeOut')
except HTTPError:
    print('Http Error')
except RequestException:
    print('Errorc')