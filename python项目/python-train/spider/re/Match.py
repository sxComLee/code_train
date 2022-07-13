'''关于匹配模式，尽量使用泛匹配模式，使用括号得到匹配目标，尽量使用非贪婪模式，有换行符就用re.S
缺点：匹配的时候先找开始的第一个字符 如果不存在，那么无法匹配'''
import re
content = 'Hello 123 4567 World_This is a Regex Demo'
contents = 'Hello 1234567 World_This' +'\n'\
           ' is a Regex Demo'

'''匹配模式'''
result = re.match('^He.*?(\d+).*$', contents,re.S)
print(result.group(1))

'''非贪婪匹配'''
# .*？ 会尽可能少的匹配直到匹配不到为止，所以 /d 最终能匹配到很多字符
result = re.match('^He.*?(\d+).*Demo$',content)
# 输出re匹配的结果
print(result)
# 输出匹配到的第一个括号里边儿的内容
print(result.group(1))

'''贪婪匹配'''
# .* 会尽可能多的匹配直到匹配不到为止，所以 /d 最终只能匹配到最后一个数字7
result = re.match('^He.*(\d+).*Demo$',content)
# 输出re匹配的结果
print(result)
# 输出匹配到的第一个括号里边儿的内容
print(result.group(1))


'''获取匹配目标'''
result = re.match('^Hello\s(\d+)\s.*Demo$',content)
# 输出re匹配的结果
print(result)
# 输出匹配到的字符串
print(result.group())
# 输出匹配到的第一个括号里边儿的内容
print(result.group(1))
# 输出匹配的长度
print(result.span())

'''泛匹配'''
result = re.match('^Hello.*Demo$', content)
# 输出re匹配的结果
print(result)
# 输出匹配到的字符串
print(result.group())
# 输出匹配的长度
print(result.span())

# 输出字符长度
print(len(content))
result = re.match('^Hello\s\d{3}\s\d{4}\s\w{10}.*Demo$', content)
# 输出re匹配的结果
print(result)
# 输出匹配到的字符串
print(result.group())
# 输出匹配的长度
print(result.span())

'''转义'''
content = 'price is $5.00'
result = re.match('price is $5.00',content)
print(result)
result = re.match('price is \$5\.00',content)
print(result)
