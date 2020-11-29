#!/usr/bin/env python
# coding: utf-8

import pyspark
import sys
import json
from pyspark import SparkContext, SparkConf


def Statistical_Data(gamergate_rdd):

    gamergate = gamergate_rdd.persist()

    mean_retweet = gamergate.map(lambda x: x['retweet_count']).mean()

    max_retweet = gamergate.map(lambda x: x['retweet_count']).max()

    stdev_retweet = gamergate.map(lambda x: x['retweet_count']).stdev()

    output_json = {"mean_retweet": mean_retweet,
    "max_retweet": max_retweet,
    "stdev_retweet":stdev_retweet}
    return output_json


if __name__ == '__main__':

    sc_conf = pyspark.SparkConf()
    sc_conf.setAppName('hw1')
    sc_conf.setMaster('local[*]')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    f_rdd = sc.textFile(input_file)
    gamergate_rdd = f_rdd.map(lambda line: json.loads(line))
    task2_json = Statistical_Data(gamergate_rdd)

    with open(output_file, 'w') as outfile:
        json.dump(task2_json, outfile)