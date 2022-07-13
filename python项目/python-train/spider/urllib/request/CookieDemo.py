# cookie代理，用来维持登陆状态的
import http.cookiejar, urllib.request

# cookie = http.cookiejar.CookieJar
# handler = urllib.request.HTTPCookieProcessor(cookie)
# opener = urllib.request.build_opener(handler)
# opener.open('http://www.baidu.com')
# # 将cookie信息打印出来
# for item in cookie:
#     print(item.name + "=" + item.value)

# 保存cookie文件信息
filename = 'lwp_cookie.txt'
# 火狐浏览器的cookie文件
cookie = http.cookiejar.LWPCookieJar(filename)
handler = urllib.request.HTTPCookieProcessor(cookie)
opener = urllib.request.build_opener(handler)
res = opener.open('http://www.baidu.com')
for item in cookie:
    print(item.name + "=" + item.value)
cookie.save(ignore_discard=True, ignore_expires=True)


# 保存cookie文件信息
filename = 'mozil_cookie.txt'
# 火狐浏览器的cookie文件
cookie = http.cookiejar.MozillaCookieJar(filename)
handler = urllib.request.HTTPCookieProcessor(cookie)
opener = urllib.request.build_opener(handler)
res = opener.open('http://www.baidu.com')
cookie.save(ignore_discard=True, ignore_expires=True)

print('================')
# 读取cookie文件
cookie.load('mozil_cookie.txt',ignore_expires=True,ignore_discard=True)
handler = urllib.request.HTTPCookieProcessor(cookie)
opener = urllib.request.build_opener(handler)
res = opener.open('http://www.baidu.com')
print(res.read().decode('utf8'))
