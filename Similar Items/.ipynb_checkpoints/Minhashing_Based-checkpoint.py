import sys
import pdb
import time
import numpy as np
import itertools

from Hash_Based import get_shingle_hash
from Locality_Sensititve_Hashing import get_hash_coeffs, min_hashing, LSH
from Nested_Loop import print_all_similar_pairs

def find_all_similiar(shingles, threshold, bucket_list, M, band_hash_size, verify_by_signature):
	candidates = set() 
	for band in bucket_list:
		for i in range(band_hash_size):
			bucket = band[i]
			if len(bucket) >= 2:
				pairs = itertools.combinations(bucket, 2)
				pair_sets = [frozenset(p) for p in pairs]
				candidates = candidates.union(pair_sets)
	print('#Candidates', len(candidates))

	candidate_list = []
	if verify_by_signature:
		for pair in candidates:
			i1, i2 = pair 
			c1 = M[:, i1]
			c2 = M[:, i2]
			sim = np.mean(c1 == c2)
			if sim >= threshold:
				if i1 >= i2: 
					i1, i2 = i2, i1
				candidate_list.append(((i1, i2), sim))
	else:
		for pair in candidates:
			i1, i2 = pair 
			c1 = shingles[i1]
			c2 = shingles[i2]
			sim = len(c1 & c2) / len(c1 | c2)
			if sim >= threshold:
				if i1 >= i2:
					i1, i2 = i2, i1
				candidate_list.append(((i1, i2), sim))

	candidate_list = sorted(candidate_list, key=lambda x: (-x[1], x[0][0], x[0][1]))
	return candidate_list

#python3 Minhashing_Based.py sample.txt 5 0.8
if __name__ == '__main__':
	filename = sys.argv[1]
	n_grams = int(sys.argv[2])
	threshold = float(sys.argv[3])

	hash_size = 2**20
	band_hash_size = 2**16
	verify_by_signature = False

	b = 10
	r = 10
	br = b * r

	time_start = time.time()

	shingles, _ = get_shingle_hash(n_grams, hash_size, filename)
	hash_coeffs = get_hash_coeffs(br)
	M = min_hashing(shingles, hash_coeffs, br)
	bucket_list = LSH(M, b, r, band_hash_size)
	sim_all_list = find_all_similiar(shingles, threshold, bucket_list, M, band_hash_size, verify_by_signature)
	print_all_similar_pairs(sim_all_list, threshold)

	time_end = time.time()
	elapsed = time_end - time_start
	print(f'Elapsed: {elapsed}')
