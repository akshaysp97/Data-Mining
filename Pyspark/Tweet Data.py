import sys
import json
from pyspark import SparkContext

sc = SparkContext("local[*]","Task1")
input_filename = sys.argv[1]
output_filename = sys.argv[2]

data = sc.textFile(input_filename)
data_json = data.map(json.loads)

n_tweet = data_json.count()

data_tuple = data_json.map(lambda x: x["user"]["id"]).distinct()
n_users = data_tuple.count()

data_tup = data_json.map(lambda x: (x["user"]["screen_name"],x["user"]["followers_count"]),1).sortBy(lambda x: x[1],False).take(3)

Tuesday_Tweet = data_json.map(lambda x: x["created_at"].split(' ')).filter(lambda x: x[0]=='Tue').count()

output = {"n_tweet": n_tweet, "n_user": n_users, "popular_users": data_tup, "Tuesday_Tweet": Tuesday_Tweet}

with open(output_filename,"w") as json_file:
	json.dump(output, json_file)

sc.stop()
