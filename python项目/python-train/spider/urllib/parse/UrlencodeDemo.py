'''urlencode将数组对象转换成get请求中的参数'''
from urllib.parse import urlencode

params = {
    'name':'jiang.li',
    'age':'22'
}

base_url = 'http://www.baidu.com?'
# http://www.baidu.com?name=jiang.li&age=22
url = base_url + urlencode(params)
print(url)