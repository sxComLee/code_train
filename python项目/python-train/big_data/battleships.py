from random import randint
from pip._vendor.distlib.compat import raw_input

# 引入随机函数
board = []
# 建一个空的list
for x in range(5):
    board.append(["O"] * 5)


# 用range()方法给list写值，每一个["0"]*5是item
def print_board(board):
    for row in board:
        print
        " ".join(row)
    # 输出list，去掉引号和括号，用空格连接


print
"Let's play Battleship!"
print_board(board)


def random_row(board):
    return randint(0, len(board) - 1)


def random_col(board):
    return randint(0, len(board[0]) - 1)


ship_row = random_row(board)
ship_col = random_col(board)

# Everything from here on should go in your for loop!
# Be sure to indent four spaces!
for turn in range(4):
    # 循环四次
    guess_row = int(raw_input("Guess Row:"))
    # 输入猜测值
    guess_col = int(raw_input("Guess Col:"))

    if guess_row == ship_row and guess_col == ship_col:
        print
        "Congratulations! You sunk my battleship!"
        break
    else:
        if (guess_row < 0 or guess_row > 4) or (guess_col < 0 or guess_col > 4):
            print
            "Oops, that's not even in the ocean."
        elif (board[guess_row][guess_col] == "X"):
            # 如果已经被填充X说明是重复值
            print
            "You guessed that one already."
        else:
            print
            "You missed my battleship!"
            board[guess_row][guess_col] = "X"
        # 猜错的值用X填充
        print(turn + 1)
        if turn == 3:
            print
            "Game Over"
    print_board(board)