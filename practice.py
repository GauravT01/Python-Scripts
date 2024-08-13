print('whats your Name ?')
myname = input()
print ('Hello '+ myname)
print (len(myname))

#if-else
name = 'aa'
if name == 'Gaurav':
    print('name matched')
else:
    print('Name not matched')

month = 'June'

month = input('Please give month = ')
if month == 'june':
    print('Its June month')
elif month == 'january':
    print ('Its january')
else:
    print('Its neither June and januray')
'''


'''
#while
name = ''
while name != 'Gaurav':
    print('please enter you Name')
    name = input()

print('Welcome Gaurav')
'''

#continue and break
'''
spam = 0
while spam<10:
    spam = spam+1
    if spam == 2:
       break
    print('spam is' + str(spam))

spam = 0
while spam<10:
    spam = spam+1
    if spam == 2:
       continue
    print('spam is' + str(spam))

'''

#for (start,end, gap kitna)
'''
spam = 0
for spam in range(2,6,2): # starts at 2 , end before 6, have gap of 2
    print ('spam is' + str(spam))

'''
'''
import random
random.randint(1,100)


'''
#built-in 
'''
#import sys
from sys import *
print('sys')
#sys.exit()
exit()
print('HI')
'''

#functions
'''
def sums(A,B):
     A= 0
     B= 0
     A= input('Enter A= ')
     B= input('Enter B= ')
NewSum = sums(A+B)
print('Newsum')

#TrainTiming func
import random

def traintiming():
    your_time = int(input('Tell me your Time of Arrival (in minutes) = '))
    train_time = random.randint(1, 60)
    print('Train arrival time (in minutes) = ', train_time)
    if your_time == train_time:
        print('You are on time. The train is arriving.')
    elif your_time > train_time:
        print('Sorry!!, You have arrived late. The train is gone.')
    elif your_time < train_time:
        print('You are early. Wait for the train to arrive.')

traintiming()


#Calculator function

opertaion = input('Please Enter the Operation (+,- , /, *) =') #Global Variable
def calculator():
        A = float(input('Please enter A = ')) #Local variable
        B = float(input('Please enter B = '))

        
        try:                                   #Try/ Except block
                        if opertaion == '+':
                            sum = A + B
                            print('Sum = ',sum)
                        elif opertaion == '-':
                            sub = A - B
                            print('Sub = ',sub)
                        elif opertaion == '*':
                            multiply = A * B
                            print('Multiply = ',multiply)
                        elif opertaion == '/':
                            divide = A / B
                            print('Divide = ',divide)
        except ZeroDivisionError :
                    print('ERROR!!!,You tried dividing by 0')


calculator()

######################   
#GuesstheNumberGame

import random
print('Welcome to play Guessing Number Game!!!')
name = input('Whats your Name = ')
print('Hello!! '+name+' hope youre doing good :)')
print('well ' +name+ ', You have to Guess a number between 1 - 20')
secretnum = random.randint(1,20) 

for guesstaken in range(1,7):
    print('Take a Guess!!')
    guess = int(input())
    
    if guess < secretnum:
        low = secretnum - guess
        if low <= 2:
            print('Your guess is low, you are close')    
        elif low > 2:
            print('Your guess is too Low')
    elif guess > secretnum:
        high = guess - 2
        if high == secretnum:
            print('Your guess is high, you are too close')    
        elif high > secretnum:
            print('Your guess is too high')
    else:
        break

if guess == secretnum:
    print('Well Done!!'+name+' you guessed number in '+str(guesstaken)+ ' guesses!')
else :
    print('Nope the number was '+ str(secretnum))

######################
#list

spam = ['man','women','child','alien','animal']

spam[0]  #result - 'man' , #here square bracket contains INDEX value
spam[1]  #result - 'women' 


#similar to Index we have Slice -> spam[1:3] -> this is slice - Multiple indexes

#slice
spam[1:3]  #result =['women', 'child']

nospam = [[1,2,3,4],['man','women','child','alien','animal']]
 #             |                    |
 #            [0]                  [1]   

nospam[1][2]  #result = women
nospam[0][2]  #result = 2

nospam[1][-3]     #result = alien
nospam[0][-3]   #result = 4

# to add in list
spam[1] = '9'   #it will update [1] position

#delete
del spam[2]

list('GAURAV')    #result = ['G', 'A', 'U', 'R', 'A', 'V']

#IN, Not In
'man' in spam  #True
'man' not in spam  #False

######################
#GUESSING NUMBER GAME

import random

def save_high_score(name, score):
    """Save the high score to a file."""
    try:
        with open("high_scores.txt", "a") as file:
            file.write(f"{name}: {score}\n")
    except IOError as e:
        print(f"Failed to save the high score: {e}")

def display_high_scores():
    """Display high scores from the file."""
    try:
        with open("high_scores.txt", "r") as file:
            scores = file.readlines()
            if scores:
                print("High Scores:")
                for line in scores:
                    print(line.strip())
            else:
                print("No high scores recorded yet.")
    except FileNotFoundError:
        print("No high scores recorded yet.")

print('Welcome to the improved Guessing Number Game!')
name = input('What is your name? ')
print(f'Hello, {name}! Try to guess the number between 1 and 20.')

display_high_scores()

secretnum = random.randint(1, 20)

for guesstaken in range(1, 7):
    try:
        guess = int(input('Take a guess: '))
        if guess < 1 or guess > 20:
            print("Guess out of bounds! Please guess a number between 1 and 20.")
            continue
    except ValueError:
        print("Invalid input. Please enter a number.")
        continue
    
    if guess < secretnum:
        difference = secretnum - guess
        if difference <= 2:
            print('Your guess is close, but a little low.')
        else:
            print('Your guess is too low.')
    elif guess > secretnum:
        difference = guess - secretnum
        if difference <= 2:
            print('Your guess is close, but a little high.')
        else:
            print('Your guess is too high.')
    else:
        break

if guess == secretnum:
    print(f'Good job, {name}! You guessed the number in {guesstaken} guesses!')
    save_high_score(name, guesstaken)
else:
    print(f'Sorry, {name}. The number was {secretnum}. Better luck next time!')

