#!/usr/bin/env python
# coding: utf-8

import pyspark
import sys
import json
from pyspark import SparkContext, SparkConf


def Extract_Data(gamergate_rdd):

    gamergate = gamergate_rdd.persist()

    # Total number of tweets
    n_tweet = gamergate.count()
  
    # Number of distinct users
    n_user = gamergate.map(lambda x: x['user']['id']).distinct().count()

    # Top 3 users with most followers
    popular_users = gamergate.map(lambda x: (x['user']['screen_name'],x['user']['followers_count'])).sortBy(lambda x: -x[1]).take(3)

    # Number of tweets created on Tuesday
    Tuesday_Tweet = gamergate.map(lambda x: x['created_at'].split(' ')[0]).filter(lambda x: x == 'Tue').count()

    output_json = {"n_tweet": n_tweet,
    "n_user": n_user,
    "popular_users":popular_users,
    "Tuesday_Tweet": Tuesday_Tweet}
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
    task1_json = Extract_Data(gamergate_rdd)

    with open(output_file, 'w') as outfile:
        json.dump(task1_json, outfile)