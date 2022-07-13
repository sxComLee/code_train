import requests

response = requests.get('https://www.baidu.com/img/bd_logo1.png')

print(response.content)

with open('/var/tmp/1.gif','wb') as f:
    f.write(response.content)
    f.close()
