from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import binascii
import sys
import datetime

def myhashs(user_str):
    m = 69997
    result = []
    list_a = [23, 37, 43]
    list_b = [19, 89, 113]
    user_val = int(binascii.hexlify(user_str.encode('utf8')), 16)
    # (ax + b) % m
    for i in range(0, 3):
        for j in range(0, 3):
            result.append((list_a[i] * user_val + list_b[j]) % m)
    return result


def bloom(rdd):
    global filter_array
    global seen_user
    false_pos = 0.0
    true_neg = 0.0
    stream = rdd.collect()
    for user_id in stream:

        index = myhashs(user_id)
        exist = True
        for bit in index:
            if filter_array[bit] == 0:
                exist = False

        # calculate FP and TN
        if user_id not in seen_user and exist:
            false_pos += 1
        if user_id not in seen_user and not exist:
            true_neg += 1

        # change the global filter array
        for bit in index:
            filter_array[bit] = 1

        # add to the set
        seen_user.add(user_id)

    # calculate FPR
    false_positive_rate = 0.0 if (false_pos + true_neg == 0) else false_pos / (false_pos + true_neg)

    with open(output, 'a') as f:
        f.write(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        f.write("," + str(float(false_positive_rate)))
        f.write("\n")
        print(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " " + str(float(false_positive_rate)))


if __name__ == "__main__":

    appName = "spark-task1"
    master = "local[*]"

    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 10)
    batch = ssc.socketTextStream("localhost", 9999)

    filter_array = [0 for i in range(69997)]
    seen_user = set()

    output = "bloomfilter_stream.csv"
    with open(output, 'w') as f:
        f.write("Time,FPR\n")

    batch.foreachRDD(bloom)
    ssc.start()
    ssc.awaitTermination()