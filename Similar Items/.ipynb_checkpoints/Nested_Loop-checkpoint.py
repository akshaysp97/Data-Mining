import sys
import pdb
import time
import numpy as np

from Dictionary_Based import find_ngrams, get_shingle_dict, find_similar_from_list
from Hash_Based import get_shingle_hash

def print_all_similar_pairs(sim_all_list, threshold):
	print(f'Reviews pairs with similarity greater than {threshold}')
	for sim in sim_all_list:
		print(f'{sim[0]}: {sim[1]}')

	print(f'Total: {len(sim_all_list)}')

#python3 Nested_Loop.py review/sample.txt 5 0.8 dict
if __name__ == '__main__':
	filename = sys.argv[1]
	n_grams = int(sys.argv[2])
	threshold = float(sys.argv[3])
	method = 'dict' 

	if len(sys.argv) >= 5 and sys.argv[4] == 'hash':
		method = 'hash'

	time_start = time.time()

	if method == 'dict':
		shingles, db = get_shingle_dict(n_grams, filename)
	else:
		hash_size = 2**20
		shingles, _ = get_shingle_hash(n_grams, hash_size, filename)

	sim_all = {}
	time_last = time.time()
	count = len(shingles)
	for i in range(count):
		if i % 100 == 0:
			print(f'{i}/{count}: Last batch time: {time.time() - time_last}')
			time_last = time.time()

		sims = find_similar_from_list(shingles, i, threshold)

		for sim in sims:
			if sim[0] != i:
				# keep pairs in sorted order
				i1 = min(i, sim[0])
				i2 = max(i, sim[0])
				sim_all[(i1, i2)] = sim[1]

	sim_all_list = sorted(sim_all.items(), key=lambda x: (-x[1], x[0][0], x[0][1]))
	print_all_similar_pairs(sim_all_list, threshold)

	time_end = time.time()
	elapsed = time_end - time_start
	print(f'Elapsed: {elapsed}')
