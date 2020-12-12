import binascii
import sys
import datetime
import statistics
from blackbox import BlackBox
import math


def myhashs(user_str):
    m = 1024
    parameters = [
        (8779, 181, 76157), (10067, 5881, 80077), (2551, 1213, 89899),
        (1217,27109, 101999), (769, 50261, 83063), (4003, 12829, 75511),
        (239, 17807, 75979), (997, 51577, 65327), (5779,34757, 57037),
        (6323, 9631, 8287), (1483, 2593, 4759), (7219, 8117, 677),
        (661, 2293, 1823), (947, 3547, 3833), (397, 9187, 8429)
    ]

    result = []
    user_id = int(binascii.hexlify(str(user_str).encode('utf8')), 16)
    for a, b, p in parameters:
        result.append(((a * user_id + b) % p)  % m)
    return result

def trailing_zeros(hash_num):
    bit=bin(hash_num)
    zero_count=0
    for i in bit[::-1]:
        if i=='0':
            zero_count+=1
        else:
            break
    return zero_count


def flajolet(stream):

    # find the ground truth
    user_set=set()
    user_list=[]

    for user_id in stream:
        user_set.add(user_id)
        user_list.append(user_id)

    ground_truth=len(user_set)

    #represent for the biggest :  k = num of hash functions = 15
    maxR = [0] * k

    for user in user_list:
        hash_values = myhashs(user)
        for j in range(0, k):
            # length of the longest trailing zeros
            maxR[j] = max(trailing_zeros(hash_values[j]), maxR[j])

    # get the count 2^r
    estimations = [math.pow(2, r) for r in maxR]

    # seperate into groups and get average
    avg_group = [0] * g # g is the num of the group, which is 5
    for i in range(0, k, l): # size of each group is l = 3
        avg_group[i // l] = int(math.fsum(estimations[i: i + l]) // l)

    # get the median
    groups = sorted(avg_group)
    estimate = (int) (statistics.median((avg_group)))

    with open(output,"a") as f:
        f.write(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        f.write(","+str(ground_truth))
        f.write(","+str(estimate))
        f.write("\n")
        print(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(ground_truth) + " " + str(estimate))



if __name__ == '__main__':
    file = "users.txt"
    num = 300
    times = 30
    output = "FlajoletMartin_output.csv"

    # num of hash_functions
    k = 15
    # groups
    g = 5
    # num in group
    l = 3
    with open(output, 'w+') as f:
        f.write("Time,Ground Truth,Estimation")
        f.write("\n")
    bx = BlackBox()
    for i in range(times):
        flajolet(bx.ask(file,num))
