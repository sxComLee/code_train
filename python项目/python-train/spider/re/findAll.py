import re
content = 'Hello 123 4567 World_This is a Regex Demo'
results = re.findall('(\d+)', content)
print(results)
print(type(results))
for result in results:
    print(result)