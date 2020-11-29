#!/usr/bin/env python
# coding: utf-8

import pyspark
import sys
import json
from pyspark import SparkContext, SparkConf


def Tweet_Data(tweet_rdd):

    words = tweet_rdd.flatMap(lambda line: line.split(" "))

    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

    max_word = wordCounts.sortBy(lambda word: -word[1]).take(1)[0]

    mindless_count = wordCounts.filter(lambda word: word[0] == "mindless").take(1)[0][1]

    chunk_count = wordCounts.filter(lambda word: word[0] == "|********************").take(1)[0][1]

    output_json = {"max_word": max_word,
    "mindless_count": mindless_count,
    "chunk_count": chunk_count}
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
    task3_json = Tweet_Data(f_rdd)

    with open(output_file, 'w') as outfile:
        json.dump(task3_json, outfile)