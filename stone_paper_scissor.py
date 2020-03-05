import random
def stone_fun():
    if player==ran:
        print("tie")
    elif player=="stone":
        if ran=="paper":
            print("pc wins")
        else:
            print("user wins")

def paper_fun():
    if player==ran:
        print("tie")
    elif player=="paper":
        if ran=="scissor":
            print("pc wins")
        else:
            print("user wins")


def scissor_fun():
    if player==ran:
        print("tie")
    elif player=="scissor":
        if ran=="stone":
            print("pc wins")
        else:
            print("user wins")
while True:
    player=input("make your move >")
    list=['stone','paper','scissor']
    ran=random.choice(['stone','paper','scissor'])
    if player.lower()  in list:
        print(f'pc choses > {ran}')
        if player=="stone":
            stone_fun()
        elif player=="paper":
            paper_fun()
        elif player=="scissor":
            scissor_fun()
    else:
        print("vachta yeta na ")
