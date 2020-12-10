import pdb
import collections
import numpy as np
import pandas as pd
import time
import sys
import os

def get_user_movie_rating_dict(filename):
	with open(filename, 'r', encoding='utf-8') as fd:
		fd.readline() 
		row_idx = []
		col_idx = []
		data_ary = []

		user_dict = {}
		movie_dict = {}
		user_mean_dict = collections.defaultdict(list)
		movie_mean_dict = collections.defaultdict(list)

		db = collections.defaultdict(dict)
		for line in fd:
			tokens = line[:-1].split(',')
			userId = tokens[0]
			movieId = tokens[1]
			rating = float(tokens[2])

			if userId not in user_dict:
				user_dict[userId] = len(user_dict)

			if movieId not in movie_dict:
				movie_dict[movieId] = len(movie_dict)

			row_idx.append(movie_dict[movieId])
			col_idx.append(user_dict[userId])

			user_mean_dict[user_dict[userId]].append(rating)
			movie_mean_dict[movie_dict[movieId]].append(rating)
			data_ary.append(rating)

		data_ary = np.asarray(data_ary)
		mean_rating = np.mean(data_ary)

		user_mean_ary = np.zeros(len(user_mean_dict), dtype=np.float32)
		for idx, ary in user_mean_dict.items():
			user_mean_ary[idx] = np.mean(ary) - mean_rating
		user_mean_ary = user_mean_ary.reshape((1, -1))
		user_mean_ary = np.repeat(user_mean_ary, len(movie_dict), 0)

		movie_mean_ary = np.zeros(len(movie_mean_dict), dtype=np.float32)
		for idx, ary in movie_mean_dict.items():
			movie_mean_ary[idx] = np.mean(ary) - mean_rating
		movie_mean_ary = movie_mean_ary.reshape((-1, 1))
		movie_mean_ary = np.repeat(movie_mean_ary, len(user_dict), 1)

		base_rating = user_mean_ary + movie_mean_ary + mean_rating
		m = np.zeros((len(movie_dict), len(user_dict)), dtype=np.float32)
		for i in range(len(data_ary)):
			m[row_idx[i]][col_idx[i]] = data_ary[i] - mean_rating - user_mean_ary[0][col_idx[i]] - movie_mean_ary[row_idx[i]][0]

		return m, user_dict, movie_dict, mean_rating, base_rating, user_mean_ary, movie_mean_ary

# python Model_Based_Movie_Recommendation.py movies.csv ratings_train.csv ratings_test.csv ratings_pred_1.csv

	time_start = time.time()

	movies_filename = sys.argv[1]
	rating_train_filename = sys.argv[2]
	rating_test_filename = sys.argv[3]
	output_filename = sys.argv[4]

	X, user_dict, movie_dict, mean_rating, base_rating, user_mean_ary, movie_mean_ary = get_user_movie_rating_dict(rating_train_filename)

	u, s, vh = np.linalg.svd(X)
	s = np.sqrt(s)
	s_u = np.zeros(X.shape)
	for i, v in enumerate(s):
		s_u[i, i] = v

	s_v = np.diag(s)

	r = 12
	A = np.matmul(u, s_u[:, :r])
	B = np.matmul(s_v[:r, :], vh)
	AB = np.matmul(A, B)
	loss = np.mean((AB - X)**2)

	X_re = AB + base_rating
	fin = open(rating_test_filename, 'r', encoding='utf-8')
	fout = open(output_filename,'w', encoding='utf-8')
	fout.write(fin.readline())

	for line in fin:
		tokens = line[:-1].split(',')
		userId = tokens[0]
		movieId = tokens[1]
		user_index = user_dict.get(userId, None)
		movie_index = movie_dict.get(movieId, None)

		if user_index is not None and movie_index is not None:
			rating = X_re[movie_index][user_index]
		else:
			if user_index is not None:
				rating = user_mean_ary[0][user_index] + mean_rating
			elif movie_index is not None:
				rating = movie_mean_ary[movie_index][0] + mean_rating
			else:
				rating = mean_rating

		rating = np.clip(rating, 0.5, 5.0)
		fout.write(f'{userId},{movieId},{rating},0\n')

	fin.close()
	fout.close()
	time_end = time.time()
	print('Elapsed', time_end-time_start)
	print(r)
