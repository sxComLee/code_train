from urllib import request,parse

url = 'http://python.org'
decodestr = 'utf8'
requestA = request.Request(url)
# 将url构造成一个request实现请求
response = request.urlopen(requestA)
# print(response.read().decode(decodestr))

#添加请求头信息
headers={
    'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    'Host':'httpbin.org'
}
# 请求体信息
dic = {'name':'Germey'}
data = bytes(parse.urlencode(dict),encoding='utf8')
req = request.Request(url=url,data=data,headers=headers,method='POST')
# 添加请求头信息另一种方式
# req.add_header('User-Agent'
#                ,'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36')
res = request.urlopen(req)
print(res.read().decode(decodestr))




