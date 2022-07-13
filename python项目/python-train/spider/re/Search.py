import re
content = 'Hello 123 4567 World_This is a Regex Demo'


'''匹配模式'''
# 匹配的时候先找开始的第一个字符 如果不存在，那么无法匹配
result = re.match('e.*?(\d+).*$', content)
# 直接在字符串中查找所有的字符，查找到第一个匹配到的结果并返回
result1 = re.search('e.*?(\d+).*$', content)
print(result)
print(result1.group(1))