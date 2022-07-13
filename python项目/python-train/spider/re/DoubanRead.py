import requests, re

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
}
get = requests.get('https://book.douban.com/',headers=headers).text
print(get)

# pattern = re.compile('<li.*cover.*?href="(.*?)".*?title="(.*?)".*?more-meta.*?author">(.*?)</span>.*year">(.*?)</span>.*?</li>',re.S)
# results = re.findall(pattern, get)
# for result in results:
#     url,name,author,date = result
#     # 将换行符替换成空字符串
#     author = re.sub('\s','',author)
#     date = re.sub('\s','',date)
#     print(url,name,author,date)
# print(results)

pattern = re.compile('<li.*?cover.*?href="(.*?)".*?</li>',re.S)
results = re.findall(pattern, get)
for result in results:
    print(result)

