import sys
import json
from pyspark import SparkContext

sc = SparkContext("local[*]","Task2")
input_filename = sys.argv[1]
output_filename = sys.argv[2]

data = sc.textFile(input_filename)
data_json = data.map(json.loads)

data_retweet = data_json.map(lambda x: x["retweet_count"]).mean()

data_max_retweet = data_json.map(lambda x: x["retweet_count"]).max()

data_std_retweet = data_json.map(lambda x: x["retweet_count"]).stdev()

output = {"mean_retweet": data_retweet, "max_retweet": data_max_retweet,"stdev_retweet": data_std_retweet}

with open(output_filename,"w") as json_file:
	json.dump(output, json_file)

sc.stop()
