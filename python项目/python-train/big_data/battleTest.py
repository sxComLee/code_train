from random import randint #从random模块中引入randint功能

from pip._vendor.distlib.compat import raw_input

board = []#创建一个空list，作为地图基础
#通过for循环，创建一个5×5的范围地图，这一步非常重要，for循环来控制循环数量，激活append的功能！
for x in range(5):
    board.append(["O"] * 5)

#这里的row，其实就是5×5里面里面的每个item，就是['O','O','O','O','O']
def print_board(board):
    # 在整个地图list内，有5个这样的item
    for row in board:
        # 在每个item内，每2个元素间添加空格
        print("".join(row))

#运行程序后开始的第一句话
print("Let's play Battleship!")

#显示地图
print_board(board)

#这一步是定义随机产生战舰的位置行坐标
def random_row(board):
    # 位置的坐标是从0到4.len(board)-1就代表了最长的坐标值
   return randint(0, len(board) - 1)

#同上，定义随机产生战舰的位置列坐标
def random_col(board):
    return randint(0, len(board[0]) - 1)

#定义完以后，就赋值给变量
ship_row = random_row(board)
ship_col = random_col(board)
print(ship_row)#这个可以显示船的坐标，注意，正常游戏的时候是不需要这样的语句出现的，纯粹是调试时候用，因为你不知道每次运行程序，船的坐标会出现在哪里
print(ship_col)

# Everything from here on should go in your for loop!
# Be sure to indent four spaces!
for turn in range(4):#这句是计算一个猜了几次，最多只能猜4次
    guess_row = int(raw_input("Guess Row:"))#以下2句是要求玩家输入位置坐标，这个功能不仅是提供了输入功能，同时，非常重要的是，他控制了for turn这个历遍
    guess_col = int(raw_input("Guess Col:"))#不是一次性历遍4次，而是一次历遍完了以后，再输入，再进行次数累加，避免了一口气历遍完range(4)的情况！
    if guess_row == ship_row and guess_col == ship_col: #判断猜测坐标和实际船坐标是否一致
        print("Congratulations! You sunk my battleship!")
        break
    else:
        if guess_row not in range(5) or guess_col not in range(5)#避免猜测范围超出地图限制 ,这里的 not in写法，记一下，基础知识需要补齐
            print("Oops, that's not even in the ocean.")
        elif(board[guess_row][guess_col] == "X"):#这一句非常重要，当猜的答案已经猜测过的时候，打印下面提示语句
            print("You guessed that one already.")
        else:
            print("You missed my battleship!")
            board[guess_row][guess_col] = "X"#这句非常重要，将你猜测的坐标值，赋给"X"，只能写.....="X"，而不能写"X"=.....  因为这是将X 赋值给坐标
        print("Turn", turn + 1) #Print (turn + 1) here!   #打印当前测试的次数
        print_board(board) #再次显示地图（包含X的地图）
        if turn == 3:#如果turn等于3（也就是猜测次数是4的时候），游戏结束
            print("Game Over")
