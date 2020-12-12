import binascii
import sys
import datetime
from blackbox import BlackBox

def myhashs(user_str):
    m = 69997 # m is the length of the filter bit array
    result = []
    list_a = [23, 37, 43]
    list_b = [19, 89, 113]
    user_val = int(binascii.hexlify(user_str.encode('utf8')), 16)
    for i in range(0, 3):
        for j in range(0, 3):
            result.append((list_a[i] * user_val + list_b[j]) % m)
    return result

def bloom(stream):
    global filter_array
    global seen_user
    false_pos = 0.0
    true_neg = 0.0
    for user_id in stream:

        index = myhashs(user_id)
        exist = True
        for val in index:
            if filter_array[val]==0:
                exist = False

        # calculate FP and TN
        if user_id not in seen_user and exist:
            false_pos += 1
        if user_id not in seen_user and not exist:
            true_neg += 1

        # change the global filter array
        for val in index:
            filter_array[val] = 1

        # add to the set
        seen_user.add(user_id)

    # calculate FPR = FP / (FP + TN)
    false_positive_rate = 0.0 if (false_pos + true_neg == 0) else false_pos / (false_pos+true_neg)

    with open(output, 'a') as f:
        f.write(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        f.write(","+str(float(false_positive_rate)))
        f.write("\n")
        print(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))+" "+str(float(false_positive_rate)))



if __name__ == '__main__':
    file = "users.txt"
    num = 100
    times = 30
    output = "Bloomfilter_output.csv"

    filter_array = [0 for i in range(69997)]
    seen_user = set()

    with open(output, 'w+') as f:
        f.write("Time,FPR")
        f.write("\n")

    from blackbox import BlackBox
    bx = BlackBox()
    for i in range(times):
        bloom(bx.ask(file, num))

