from urllib.parse import urlunparse
data = {'http','www.baidu.com','index.html','user','a=6','comment'}
# 将给定的数组组合成url，拼接url
print(urlunparse(data))