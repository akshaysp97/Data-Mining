import pdb
import collections
import numpy as np
import pandas as pd
import time
import sys

def get_user_movie_rating_dict(filename):
	with open(filename, 'r', encoding='utf-8') as fd:
		fd.readline()
		db = collections.defaultdict(dict)
		for line in fd:
			tokens = line[:-1].split(',')
			userId = tokens[0]
			movieId = tokens[1]
			rating = float(tokens[2])
			db[userId][movieId] = rating

		return db

def get_hash_coeffs(br):
	rnds = np.random.choice(2**6, (2, br), replace=False)
	c = 67
	return rnds[0], rnds[1], c

def min_hashing(shingles, hash_coeffs, br):
	count = len(shingles)

	(a, b, c) = hash_coeffs
	a = a.reshape(1, -1)
	M = np.zeros((br, count), dtype=int) 
	for i, s in enumerate(shingles):
		# All shingles in the document
		row_idx = np.asarray(list(s)).reshape(-1, 1)
		m = (np.matmul(row_idx, a) + b) % c
		m_min = np.min(m, axis=0) 
		M[:, i] = m_min

	return M

def LSH(M, b, r, band_hash_size):
	count = M.shape[1]

	bucket_list = []
	for band_index in range(b):
		m = collections.defaultdict(set)
		row_idx = []
		col_idx = []

		row_start = band_index * r
		for c in range(count):
			v = M[row_start:(row_start+r), c]
			v_hash = hash(tuple(v.tolist())) % band_hash_size
			m[v_hash].add(c)
		bucket_list.append(m)

	return bucket_list

# python Content_Based_Movie_Recommendation.py movies.csv ratings_train.csv ratings_test.csv ratings_pred.csv
if __name__ == '__main__':
	time_start = time.time()

	movies_filename = sys.argv[1]
	rating_train_filename = sys.argv[2]
	rating_test_filename = sys.argv[3]
	output_filename = sys.argv[4]

	db = get_user_movie_rating_dict(rating_train_filename)

	movie_dict = {} 
	movie_dict_inv = {}
	movie_list = []
	vocab = {}

	movies = pd.read_csv(movies_filename)
	for index, row in movies.iterrows():
		movieId = str(row['movieId'])
		genres = row['genres']
		genres_list = genres.split('|')

		doc = set()
		for genre in genres_list:
			if genre not in vocab:
				vocab[genre] = len(vocab)
			doc.add(vocab[genre])

		movie_dict[movieId] = index
		movie_dict_inv[index] = movieId
		movie_list.append(doc)

	b = 10
	r = 2
	br = b*r
	threshold = 0.2
	band_hash_size = 2**16
	verify_by_signature = False
	use_lsh = True

	if use_lsh:
		hash_coeffs = get_hash_coeffs(br)
		M = min_hashing(movie_list, hash_coeffs, br)
		bucket_list = LSH(M, b, r, band_hash_size)

	fin = open(rating_test_filename, 'r', encoding='utf-8')
	fout = open(output_filename,'w', encoding='utf-8')
	fout.write(fin.readline())
	lsh_dict = {}

	for line in fin:
		tokens = line[:-1].split(',')
		userId = tokens[0]
		movieId = tokens[1]
		query_index = movie_dict[movieId]
		watched_movies = db[userId]
		watched_movies_index = set([movie_dict[x] for x in watched_movies])

		if use_lsh:
			if query_index in lsh_dict:
				candidates = lsh_dict[query_index]
			else:
				# Step 1: Find candidates
				candidates = set()
				for band_index in range(b):
					row_start = band_index * r
					v = M[row_start:(row_start+r), query_index]
					v_hash = hash(tuple(v.tolist())) % band_hash_size

					m = bucket_list[band_index]
					bucket = m[v_hash]
					candidates = candidates.union(bucket)
				lsh_dict[query_index] = candidates

			candidates = candidates & watched_movies_index
		else:
			candidates = watched_movies_index

		sims = []
		if verify_by_signature:
			query_vec = M[:, query_index]
			for col_idx in candidates:
				col = M[:, col_idx]
				sim = np.mean(col == query_vec) 
				if sim >= threshold:
					sims.append((col_idx, sim))
		else:
			query_set = movie_list[query_index]
			for col_idx in candidates:
				col_set = movie_list[col_idx]

				sim = len(query_set & col_set) / len(query_set | col_set) # Jaccard Similarity
				if sim >= threshold:
					sims.append((col_idx, sim))

		rating_list = []
		weight_list = []
		for col_idx, sim in sims:
			rating_list.append(db[userId][movie_dict_inv[col_idx]])
			weight_list.append(sim)

		if sum(weight_list) > 0:
			sum_rating = sum(i[0] * i[1] for i in zip(rating_list, weight_list)) / sum(weight_list)
		else:
			sum_rating = np.mean(list(db[userId].values()))

		sum_rating = np.clip(sum_rating, 0.5, 5.0)
		fout.write(f'{userId},{movieId},{sum_rating},0\n')

	fin.close()
	fout.close()
	time_end = time.time()
	print('Elapsed', time_end-time_start)
