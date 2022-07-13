'''代理设置'''
import requests
proxies = {
    'http':'127.0.0.1:9743',
    'https':'127.0.0.1:9743'
}
res = requests.get('https://www.taobao.com', property=proxies)
print(res.status_code)

# 如果代理有用户名和密码
proxies = {
    'http':'http://user:password@127.0.0.1:9743'
}
res = requests.get('https://www.taobao.com', property=proxies)
print(res.status_code)

# 如果代理是sock4或者sock5
proxies = {
    'http':'sock5://127.0.0.1:9743'
}
res = requests.get('https://www.taobao.com', property=proxies)
print(res.status_code)