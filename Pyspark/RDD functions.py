#!/usr/bin/env python
# coding: utf-8

# Initialize spark context
from pyspark import SparkContext

# Note the SparkContext should be a singleton(only run once). You have to stop the old context before start a new one.
sc = SparkContext("local[*]","PySpark Tutorial")  # Use all cores
#sc = SparkContext("local","PySpark Tutorial")    # One worker process, use one core
#sc = SparkContext("local[4]","PySpark Tutorial") # Use 4 cores
#sc.stop()                                        # Stop spark context

# Parallelizing an existing collection
data = [1, 2, 3, 3, 4, 5]
data_parallel = sc.parallelize(data)
print(data_parallel) # a pointer to the data in spark backend

# Multiply each element by 2
data_map = data_parallel.map(lambda x: x * 2)
print(data_map) #Lazy evaluation. The `.map` only record what to do with the data, not actually doing it
print(data_map.collect()) #The `collect()` method will trigger the actual execution

# Sum of the elements
data_reduce = data_parallel.reduce(lambda a, b: a + b) #data_reduce is aleady the result, because `.reduce()` is an Action
print(data_reduce)

# Keep the odd numbers
data_filter = data_parallel.filter(lambda x: x % 2)
print(data_filter.collect())

# one-to-many mapping
data_flatMap = data_parallel.flatMap(lambda x: [x] * x)
print(data_flatMap.collect())

# map an element to a tuple
data_tuple = data_parallel.map(lambda x: (x, 1))
print(data_tuple.collect())

# reduce pair based on key
data_reduceByKey = data_tuple.reduceByKey(lambda a, b: a+b) #a, b are value only
print(data_reduceByKey.collect())

# sortByValue
data_sort = data_reduceByKey.sortBy(lambda x: x[1], False) #x is (key, value). Default to ascending order, add False for descending order
print(data_sort.collect())

# sortByKey
data_sortByKey = data_reduceByKey.sortByKey(False)
print(data_sortByKey.collect())

# min, max, mean, std (like pandas)
print(data_parallel.min(), data_parallel.max(), data_parallel.mean(), data_parallel.stdev()) # Note they are distributed algorithm



# Using Spark api to reference a dataset
lines = sc.textFile("train.csv")
print(lines) # a pointer to the data in spark backend

# How many lines?
n_lines = lines.count()
print(n_lines)

# Which sentence have the most words? How many?
max_words = lines.map(lambda x: (len(x.split(' ')), x)).sortBy(lambda x: x[0], False)
print(max_words.take(1))

# How many words in a sentence is the most common?
sorted_words = lines.map(lambda x: (len(x.split(' ')), 1)).reduceByKey(lambda a, b: a+b).sortBy(lambda x: x[1], False)
print(sorted_words.take(1))


# Estimating Pi

import random
import time
from pyspark import SparkContext, SparkConf

NUM_SAMPLES = 20000000  
def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

try:
    sc
except NameError:
    sc = SparkContext("local[*]","PySpark Tutorial")

time_start = time.time()
count = sc.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()
pi = 4.0 * count / NUM_SAMPLES
time_end = time.time()
elapsed = time_end - time_start

print(f'Estimated Pi: {pi}')
print(f'Elapsed: {elapsed}')


# Most frequent words

import sys
import time

from pyspark import SparkContext

try:
    sc
except NameError:
    sc = SparkContext("local[*]","PySpark Tutorial")

time_start = time.time()
# read data from text file and split each line into words
words = sc.textFile('train.csv').flatMap(lambda line: line.split(" "))

# count the occurrence of each word
frequent_list = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b:a + b).sortBy(lambda x: x[1], False).take(10)

time_end = time.time()
elapsed = time_end - time_start

print(f'Most frquent words: {frequent_list}')
print(f'Elapsed: {elapsed}')