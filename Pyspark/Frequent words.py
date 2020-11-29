import sys
import json
from pyspark import SparkContext

sc = SparkContext("local[*]","Task3")
input_filename = sys.argv[1]
output_filename = sys.argv[2]

data = sc.textFile(input_filename).flatMap(lambda line: line.split(' '))

words = data.map(lambda word: (word,1)).reduceByKey(lambda a, b:a + b).sortBy(lambda x: x[1], False).take(1)

mind = data.map(lambda x: (x,1)).filter(lambda x: x[0]=="mindless").count()

chunk_size=data.filter(lambda x: x.startswith('|********************')).count()

output = {"max_word": words, "mindless_count": mind,"chunk_count": chunk_size}

with open(output_filename,"w") as json_file:
	json.dump(output, json_file)

sc.stop()

