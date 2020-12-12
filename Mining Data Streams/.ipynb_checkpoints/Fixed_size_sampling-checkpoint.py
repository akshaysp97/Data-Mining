import sys
import random
from blackbox import BlackBox
import time

def sample(stream):

    global size # size of the memory 100
    global num_of_users # count
    global memory #output -> list

    for user in stream:
        num_of_users += 1
        if(num_of_users <= size): 
            memory.append(user)
        else: 
            if(float(random.randint(0,100000) % num_of_users) < size):
                replace=random.randint(0,100000) % size
                memory[replace] = user

        if(num_of_users % size == 0):
            with open(output, 'a') as f:
                f.write("\n")
                f.write(str(num_of_users)+","+str(memory[10])+","+str(memory[30])+","+str(memory[50])+","+str(memory[70]+","+str(memory[90])))
            print(str(num_of_users)+","+str(memory[10])+","+str(memory[30])+","+str(memory[50])+","+str(memory[70]+","+str(memory[90])))


if __name__ == '__main__':

    file = "users.txt"
    num = 100
    time = 30
    output = "Sampling_output.csv"

    size = 100
    memory = []
    num_of_users = 0

    with open(output,'w+') as f:
        f.write("seqnum,10_id,30_id,50_id,70_id,90_id")
    random.seed(553)

    bx = BlackBox()
    for i in range(time):
        line = bx.ask(file, num)
        sample(line)

