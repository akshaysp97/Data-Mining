import sys
import pdb
import bisect
import collections
import itertools
from Triangular_Matrix_Method import get_baskets_python, get_baskets_spark, get_item_dict, filter_basket, tuple_wrapper, get_possible_k, print_frequent_itemsets

def dict_method(baskets, support, item_dict=None, k=2):
    if item_dict is None:
        item_dict = get_item_dict(baskets)  #item -> integer
    else:
        filter_basket(baskets, item_dict, k)

    if k >= 3:
        possible_k = get_possible_k(item_dict, k)

    # key is a frozenset
    db = collections.defaultdict(int)

    for basket in baskets:
        # Take a basket (user), iterate all items (artist)
        items = basket[1]

        for kpair in itertools.combinations(items, k):
            pair_set = frozenset(kpair)

            # kpair is a k element tuple, kpair[i] is item (string)
            if k >= 3:
                # Now kpair is a 2 element pair
                if pair_set not in possible_k:
                    continue

            db[pair_set] += 1

    # Extract results
    frequent_itemset_list = []
    for item_all, count in db.items():
        # apply support threshold
        if count >= support:
            item_all = tuple(sorted(list(item_all)))
            frequent_itemset_list.append((item_all, count))

    # First sorted by the occurrence count in decreasing order
    # Then sort by ascii order of the first item, in ascending order
    # Then sort by ascii order of the second item, in ascending order
    frequent_itemset_list = sorted(frequent_itemset_list, key=lambda x: [-x[1]] + list(x[0]))
    return frequent_itemset_list

# python Dict_Method.py 10 sample.tsv
if __name__ == '__main__':
    support = int(sys.argv[1])
    filename = sys.argv[2]

    if len(sys.argv) >= 4 and sys.argv[3] == 'spark':
        baskets = get_baskets_spark(filename)
    else:
        baskets = get_baskets_python(filename)

    # call the algorithm function
    itemsets = dict_method(baskets, support)
    print_frequent_itemsets(itemsets)
