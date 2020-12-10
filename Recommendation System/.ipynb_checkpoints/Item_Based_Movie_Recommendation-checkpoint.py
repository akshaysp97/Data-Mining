import pdb
import collections
import numpy as np

from User_Based_Movie_Recommendation import jaccard_similarity, cosine_similarity, centred_cosine_similarity, pearson_similarity, \
               get_user_movie_rating_dict, get_similarity_dict, predict_ratings

if __name__ == '__main__':
	N_TOP = 5
	K_NEIGHBORS = 10
	SIM_FUNCTION = pearson_similarity
	test_movie_ids = ['1', '2', '3']

	db, user_set = get_user_movie_rating_dict('ratings.csv', user_based=False)
	user_list = list(user_set)
	user_list.sort() # To keep a consistent order

	sim_db = get_similarity_dict(db, test_movie_ids, SIM_FUNCTION)
	rec_db = predict_ratings(db, sim_db, user_list, test_movie_ids, N_TOP, K_NEIGHBORS)

	for test_movie_id, rec_ary in rec_db.items():
		print(f'Recommended users for movie {test_movie_id}:')
		print(rec_ary)
