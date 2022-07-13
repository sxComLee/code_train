'''将正则字符串编译成正砸表达式对象'''
import re

content = 'Hello 123 4567 World_This is a Regex Demo'
pattern = re.compile('Hello.*Demo', re.S)
result = re.match(pattern, content)
result1 = re.match('Hello.*Demo', content,re.S)
print(result)
print(result1)
