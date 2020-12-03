import sys
import pdb
import collections
from pyspark import SparkContext
from Triangular_Matrix_Method import get_baskets_spark_rdd, triangular_matrix_method, tuple_wrapper
from Tuple_List_Method import tuple_list_method
from Dict_Method import dict_method
from A-Priori_All_Method import aprior_all_method, print_all_frequent_itemsets

# Count occurrence of candidates in each parition of the dataset
def candidate_count(baskets, candidates):
    item_counter = collections.defaultdict(int)

    for basket in baskets:
        items = frozenset(basket[1])
        for candidate in candidates:
            if items.issuperset(tuple_wrapper(candidate)):
                item_counter[candidate] += 1

    return item_counter.items()

# python SON_Method.py 7 sample.tsv 2
if __name__ == '__main__':
    support = int(sys.argv[1])
    filename = sys.argv[2]
    partition = int(sys.argv[3])

    if len(sys.argv) >= 5:
        if sys.argv[4] == 'matrix':
            method = triangular_matrix_method
        elif sys.argv[4] == 'dict':
            method = dict_method
        else:
            method = tuple_list_method
    else:
        method = tuple_list_method

    sc = SparkContext(f"local[{partition}]", "PySpark Tutorial")
    baskets_rdd = get_baskets_spark_rdd(filename, sc, partition) #baskets are RDD now!
    total_baskets = baskets_rdd.count()

    #Pass 1, candidates from each subsets
    candidates = baskets_rdd.mapPartitions(lambda baskets: aprior_all_method(baskets, support, method, True, total_baskets)).distinct().collect()

    #Pass 2, count candidates' occurence in whole datset
    itemsets = baskets_rdd.mapPartitions(lambda baskets: candidate_count(baskets, candidates)).reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] >= support).collect()

    #Post processing, organize by k (size of itemsets)
    itemsets_dict = collections.defaultdict(list)
    for itemset in itemsets:
        k = len(tuple_wrapper(itemset[0]))
        itemsets_dict[k].append(itemset)

    itemsets_list = sorted(list(itemsets_dict.items()), key=lambda x: x[0])
    itemsets_list = [sorted(t[1], key=lambda x: [-x[1]] + list(x[0])) for t in itemsets_list]

    print_all_frequent_itemsets(itemsets_list)
    sc.stop()