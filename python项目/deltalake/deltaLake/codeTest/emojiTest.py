from emoji import core
# is_emoji = emoji.is_emoji("👍")
# is_emoji2 = emoji.is_emoji("🌹")
# print(is_emoji)
# print(is_emoji2)

# emoji.is_emoji()

arg = "👍🌹"

# 假设全是 emoji 表情
result = True
# 将字符串中的空格替换掉
arg_fix = arg.replace(" ", "")
if (len(arg_fix) > 0):
    # 将字符切割成一个个的,只要出现非moji表情就返回False
    for str in arg_fix:
        if not str:
            result = False
            break
        if core.is_emoji(str):
            continue
        else:
            result = False
            break
else:
    result = False

print(result)