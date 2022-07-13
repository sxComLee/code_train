import requests

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'
}
res = requests.get('http://www.jianshu.com',headers=headers)
print(type(res.status_code),res.status_code)
print(type(res.headers),res.headers)
print(type(res.cookies),res.cookies)
print(type(res.url),res.url)
print(type(res.history),res.history)

