import sys
import pdb
import collections
from Triangular_Matrix_Method import get_baskets_python, get_baskets_spark, triangular_matrix_method, print_frequent_itemsets
from Tuple_List_Method import tuple_list_method
from Dict_Method import dict_method

# Count occurrence of each item
def get_item_counter(baskets):
    item_counter = collections.Counter()
    for basket in baskets:
        items = basket[1]
        item_counter.update(items)
    return item_counter

def get_item_dict_threshold(item_counter, support):
    item_dict = {}
    for k, v in item_counter.items():
        if v >= support:
            item_dict[k] = len(item_dict)
    return item_dict

def aprior_method(baskets, support, method):
    item_counter = get_item_counter(baskets)
    item_dict = get_item_dict_threshold(item_counter, support)
    return method(baskets, support, item_dict)

# python A-Priori Method.py 10 lastfm/sample.tsv
if __name__ == '__main__':
    support = int(sys.argv[1])
    filename = sys.argv[2]

    if len(sys.argv) >= 4 and sys.argv[3] == 'spark':
        baskets = get_baskets_spark(filename)
    else:
        baskets = get_baskets_python(filename)

    if len(sys.argv) >= 5:
        if sys.argv[4] == 'matrix':
            method = triangular_matrix_method
        elif sys.argv[4] == 'dict':
            method = dict_method
        else:
            method = tuple_list_method
    else:
        method = tuple_list_method

    # call the algorithm function
    itemsets = aprior_method(baskets, support, method)
    print_frequent_itemsets(itemsets)