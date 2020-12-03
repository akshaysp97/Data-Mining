import sys
import pdb
import bisect
import collections
import itertools
from Triangular_Matrix_Method import get_baskets_python, get_baskets_spark, get_item_dict, filter_basket, inverse_dict, tuple_wrapper, get_possible_k, print_frequent_itemsets

class FirstList(collections.UserList):
    def __lt__(self, other):
        return self[0].__lt__(other)

def tuple_list_method(baskets, support, item_dict=None, k=2):
    if item_dict is None:
        item_dict = get_item_dict(baskets)
    else:
        filter_basket(baskets, item_dict, k)

    item_dict_inv = inverse_dict(item_dict)
    n = len(item_dict)

    if k >= 3:
        possible_k = get_possible_k(item_dict, k)

    tuples = [] 

    # Key logic: Tuple List Method
    for basket in baskets:
        items = basket[1]
        for kpair in itertools.combinations(items, k):
            # kpair is a k element tuple, kpair[i] is item (string)
            if k >= 3:
                pair_set = frozenset(kpair)

                # Now kpair is a 2 element pair
                kpair = possible_k.get(pair_set, None)
                if kpair is None:
                    continue

            i = item_dict[kpair[0]]
            j = item_dict[kpair[1]]

            if i > j:
                j, i = i, j
            idx = i*n+j
            insert_idx = bisect.bisect_left(tuples, idx)

            if insert_idx >= len(tuples):
                tuples.append(FirstList([idx, 1]))
            else:
                tp = tuples[insert_idx]

                # This pair is already in the tuple list. Increase it's count (second element) by 1
                if tp[0] == idx:
                    tp[1] += 1
                else:
                    # This pair is not yet in the tuple list. Add a new tuple, the format is: (1D index, count)
                    tuples.insert(insert_idx, FirstList([idx, 1]))

    # Extract results
    frequent_itemset_list = []
    for tp in tuples:
        count = tp[1]

        # Convert 1D index to 2D index
        i = tp[0] // n
        j = tp[0] % n

        item_i = item_dict_inv[i]
        item_j = item_dict_inv[j]

        # This implementation is ready for k>=3
        item_all = set()
        for item in (item_i, item_j):
            item_all = item_all.union(tuple_wrapper(item))

        item_all = tuple(sorted(list(item_all)))

        # apply support threshold
        if count >= support:
            frequent_itemset_list.append((item_all, count))

    frequent_itemset_list = sorted(frequent_itemset_list, key=lambda x: [-x[1]] + list(x[0]))
    return frequent_itemset_list

# python Tuple_List_Method.py 10 sample.tsv
if __name__ == '__main__':
    support = int(sys.argv[1])
    filename = sys.argv[2]

    if len(sys.argv) >= 4 and sys.argv[3] == 'spark':
        baskets = get_baskets_spark(filename)
    else:
        baskets = get_baskets_python(filename)

    # call the algorithm function
    itemsets = tuple_list_method(baskets, support)
    print_frequent_itemsets(itemsets)