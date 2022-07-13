str = 'hello word bye word hello hadoop bye hadoop'

str_list = str.replace('\n','').lower().split(' ')
count_dic = {}

for str in str_list:
    if str in count_dic.keys():
        count_dic[str] = count_dic[str]+1
    else:
        count_dic[str] =1

print(count_dic)
