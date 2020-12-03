import sys
import pdb
import time
import itertools
import collections
import json

def get_baskets_python(filename):
    basket_name = None # userId, owner of the bucket
    basket = []
    baskets = []

    for line in open(filename, 'r', encoding='utf-8'):
        tokens = [t for t in line[:-1].split(',') if t != '']

        if len(tokens) == 4:
            userId, movieId, rating, timestamp = tokens
            if rating == '5.0':
                userId = int(userId)
                movieId = int(movieId)

                if userId != basket_name:
                    if basket_name is not None:
                        baskets.append((basket_name, basket))
                    basket = [movieId]
                    basket_name = userId
                else:
                    basket.append(movieId)

    baskets.append((basket_name, basket))
    return baskets

def get_item_set(baskets):
    item_set = set()
    for basket in baskets:
        item_set.union(baskets[1]) #basket[0] is userId, basket[1] is a list of movieId
    return item_set

# Count occurrence of each item
def get_item_counter(baskets):
    item_counter = collections.Counter()
    for basket in baskets:
        items = basket[1]
        item_counter.update(items)
    return item_counter

def get_possible_k(item_set, k):
    possible_k = set()
    for pair in itertools.combinations(item_set, 2):
        pair_set = set()
        for i in range(2):
            pair_set = pair_set.union(pair[i])
        if len(pair_set) == k:
            possible_k.add(frozenset(pair_set))
    return possible_k

def filter_basket(baskets, item_set, k):
    if k == 2:
        possible_item = item_set
    else:
        possible_item = set().union(*item_set)

    for i in range(len(baskets)):
        basket = baskets[i]
        items = basket[1]
        items_filterd = [item for item in items if item in possible_item]
        baskets[i] = (basket[0], items_filterd)

def dict_method(baskets, support, item_set=None, k=2):
    if item_set is None:
        item_set = get_item_set(baskets)  #item -> integer
    else:
        filter_basket(baskets, item_set, k)

    if k >= 3:
        possible_k = get_possible_k(item_set, k)

    # key is a frozenset
    db = collections.defaultdict(int)

    for basket in baskets:
        # Take a basket (user), iterate all items (movie)
        items = basket[1]
        for kpair in itertools.combinations(items, k):
            itemset = frozenset(kpair)

            if k >= 3:
                if itemset not in possible_k:
                    continue

            db[itemset] += 1

    for itemset in list(db.keys()):
        count = db[itemset]
        # apply support threshold
        if count < support:
            del db[itemset]

    return db

def aprior_all_method(baskets, support, method):
    item_counter = get_item_counter(baskets)
    itemsets = {k: v for k, v in item_counter.items() if v >= support}
    itemsets_list = [itemsets]
    frequent_last = set(itemsets.keys())

    k = 2
    while True:
        # baskets will be modfied!
        itemsets = method(baskets, support, frequent_last, k=k)
        if len(itemsets) > 0:
            itemsets_list.append(itemsets)
            frequent_last = set(itemsets.keys())
            k += 1
        else:
            break

    return itemsets_list

def get_rules(itemsets_list, I_threshold, n):
    rules = []
    len_list = len(itemsets_list)
    for i in range(len_list-1, 0, -1):
        for itemset, count in itemsets_list[i].items():
            for j in itertools.combinations(itemset, 1):
                I = itemset - set(j)
                j = j[0]

                #For size 1 itemset, it's stored as raw element rather than set
                if len(I) == 1:
                    I = list(I)[0]

                support_Ij = count
                support_I = itemsets_list[i-1][I]
                support_j = itemsets_list[0][j]
                pr_j = support_j / n

                conf_Ij = support_Ij / support_I
                interest_Ij = conf_Ij - pr_j

                if interest_Ij >= I_threshold:
                    # If I is in unboxed form
                    if type(I) is int:
                        I = [I]
                    else:
                        I = sorted(list(I))
                    rules.append([I, j, interest_Ij, support_Ij])

    rules.sort(key=lambda x: (-x[2], -x[3], x[0], x[1]))
    return rules

if __name__ == '__main__':
    time_start = time.time()

    input_filename = sys.argv[1]
    output_filename = sys.argv[2]
    I = float(sys.argv[3])
    S = int(sys.argv[4])

    baskets = get_baskets_python(input_filename)
    itemsets_list = aprior_all_method(baskets, S, dict_method)
    rules = get_rules(itemsets_list, I, len(baskets))
    with open(output_filename, 'w') as fout:
        json.dump(rules, fout)

    time_end = time.time()
    print('Elapsed', time_end-time_start)