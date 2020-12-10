import sys
import pdb
import time
import numpy as np
import collections

from Dictionary_Based import print_similar_items
from Hash_Based import get_shingle_hash

def get_hash_coeffs(br):
	#hash(x) = (a*x + b) % c
	rnds = np.random.choice(2**10, (2, br), replace=False)
	c = 1048583
	return rnds[0], rnds[1], c

def min_hashing(shingles, hash_coeffs, br):
	count = len(shingles)

	(a, b, c) = hash_coeffs
	a = a.reshape(1, -1)
	M = np.zeros((br, count), dtype=int) 
	for i, s in enumerate(shingles):
		row_idx = np.asarray(list(s)).reshape(-1, 1)
		m = (np.matmul(row_idx, a) + b) % c
		m_min = np.min(m, axis=0) #For each hash function, minimum hash value for all shingles
		M[:, i] = m_min

	return M

def LSH(M, b, r, band_hash_size):
	count = M.shape[1]

	bucket_list = []
	for band_index in range(b):
		m = collections.defaultdict(set)

		row_start = band_index * r
		for c in range(count):
			v = M[row_start:(row_start+r), c]
			v_hash = hash(tuple(v.tolist())) % band_hash_size
			m[v_hash].add(c)

		bucket_list.append(m)

	return bucket_list

def find_similiar(shingles, query_index, threshold, bucket_list, M, b, r, band_hash_size, verify_by_signature):
	# Step 1: Find candidates
	candidates = set()
	for band_index in range(b):
		row_start = band_index * r
		v = M[row_start:(row_start+r), query_index]
		v_hash = hash(tuple(v.tolist())) % band_hash_size

		m = bucket_list[band_index]
		bucket = m[v_hash]
		print(f'Band: {band_index}, candidates: {bucket}')
		candidates = candidates.union(bucket)

	print(f'Found {len(candidates)} candidates')

	# Step 2: Verify similarity of candidates
	sims = []
	if verify_by_signature:
		query_vec = M[:, query_index]
		for col_idx in candidates:
			col = M[:, col_idx]
			sim = np.mean(col == query_vec) # Jaccard Similarity is proportional to the fraction of the minhashing signature they agree
			if sim >= threshold:
				sims.append((col_idx, sim))
	else:
		query_set = shingles[query_index]
		for col_idx in candidates:
			col_set = shingles[col_idx]

			sim = len(query_set & col_set) / len(query_set | col_set) # Jaccard Similarity
			if sim >= threshold:
				sims.append((col_idx, sim))

	sims = sorted(sims, key=lambda x: x[1], reverse=True)
	return sims

#python3 Locality_Sensitive_Hashing sample.txt 467 5 0.4
if __name__ == '__main__':
	filename = sys.argv[1]
	query_index = int(sys.argv[2])
	n_grams = int(sys.argv[3])
	threshold = float(sys.argv[4])

	hash_size = 2**20      # hashtable size for k-shingle
	band_hash_size = 2**16 # hashtable size for each band, could use smaller bucket size
	verify_by_signature = False

	b = 30
	r = 3
	br = b*r

	time_start = time.time()

	shingles, _ = get_shingle_hash(n_grams, hash_size, filename)

	hash_coeffs = get_hash_coeffs(br)

	M = min_hashing(shingles, hash_coeffs, br) #col are docs, row are signature index
	bucket_list = LSH(M, b, r, band_hash_size) #list of sparse matrix
	sims = find_similiar(shingles, query_index, threshold, bucket_list, M, b, r, band_hash_size, verify_by_signature)
	print_similar_items(sims, query_index, threshold)

	time_end = time.time()
	elapsed = time_end - time_start
	print(f'Elapsed: {elapsed}')
