import pdb
import collections
import numpy as np

def get_user_movie_rating_dict(filename, user_based=True):
	with open(filename, 'r', encoding='utf-8') as fd:
		fd.readline()
		db = collections.defaultdict(dict)
		movie_set = set()
		for line in fd:
			tokens = line[:-1].split(',')
			if user_based:
				userId = tokens[0]
				movieId = tokens[1]
			else:
				userId = tokens[1]
				movieId = tokens[0]
			rating = float(tokens[2])

			db[userId][movieId] = rating
			movie_set.add(movieId)

		return db, movie_set

def get_similarity_dict(db, id_list, similarity_func):
	sim_db = {}
	for target_user in id_list:
		sim_ary = []
		target_dict = db[target_user]
		for current_user in db:
			if current_user != target_user:
				current_dict = db[current_user]
				sim = similarity_func(target_dict, current_dict)
				sim_ary.append([current_user, sim])

		sim_ary.sort(key=lambda x: x[1], reverse=True)
		sim_db[target_user] = sim_ary

	return sim_db

def jaccard_similarity(d1, d2):
	d1_k = d1.keys()
	d2_k = d2.keys()
	return len(d1_k & d2_k) / len(d1_k | d2_k)

def cosine_similarity(d1, d2):
	"""
	Compute cosine similarity of user1 and user2 using dot(user1, user2) / (norm(user1) * norm(user2))
	"""
	common_key = d1.keys() & d2.keys() 
	if len(common_key) > 0:
		dot = sum([d1[k] * d2[k] for k in common_key])
		norm1 = np.linalg.norm(list(d1.values())) 
		norm2 = np.linalg.norm(list(d2.values()))
		norm_12 = norm1 * norm2
		if norm_12 == 0:
			return 0.0
		else:
			sim = dot / norm_12
			return sim
	else:
		return 0.0

def centred_cosine_similarity(d1, d2):
	"""
	Compute cosine similarity after subtracting avg of user ratings for each user
	"""
	m1 = np.mean(list(d1.values()))
	m2 = np.mean(list(d2.values()))

	d1 = {k: v - m1 for k, v in d1.items()}
	d2 = {k: v - m2 for k, v in d2.items()}

	return cosine_similarity(d1, d2)

def pearson_similarity(d1, d2):
	"""
	Similarity based on co-rated items
	"""
	common_key = d1.keys() & d2.keys()
	if len(common_key) >= 2:
		d1 = {k: v for k, v in d1.items() if k in common_key}
		d2 = {k: v for k, v in d2.items() if k in common_key}
		return centred_cosine_similarity(d1, d2)
	else:
		return 0.0

def predict_ratings(db, sim_db, movie_list, test_user_ids, N_TOP, K_NEIGHBORS):
	rec_db = {}
	for test_user_id in test_user_ids: # For each user in the test set
		rec_ary = [] # List of recommended movies
		r_a = np.mean(list(db[test_user_id].values()))
		for movie_id in movie_list: # Predict rating for all unseen movies
			if movie_id not in db[test_user_id]: #only predicts movies user don't have a rating
				num_ary = [] #each term in numerator
				den_ary = [] #each term in denominator
				for user_id, sim in sim_db[test_user_id]: # Compare all neighbors, until K_NEIGHBORS
					if sim > 0: #Only consider positve similarity
						r_ui = db[user_id].get(movie_id, None) #rating for user u for movie i
						if r_ui is not None: #Has rating
							r_u = np.mean([v for k, v in db[user_id].items() if k != movie_id]) #Average rating for user u. Exclude the movie being computed.

							num_ary.append((r_ui - r_u) * sim)
							den_ary.append(sim)
							if len(den_ary) >= K_NEIGHBORS: #Stop once reach K_NEIGHBORS
								break

				if len(den_ary) == 0: #No other user have viewed this movie
					p_ai = r_a
				else:
					p_ai = r_a + sum(num_ary) / sum(den_ary)

				rec_ary.append([movie_id, p_ai])

		rec_ary.sort(key=lambda x: x[1], reverse=True)
		rec_db[test_user_id] = rec_ary[:N_TOP]

	return rec_db

if __name__ == '__main__':
	N_TOP = 5
	K_NEIGHBORS = 10
	SIM_FUNCTION = pearson_similarity
	test_user_ids = ['1', '2', '3']

	db, movie_set = get_user_movie_rating_dict('ratings.csv')
	movie_list = list(movie_set)
	movie_list.sort() # To keep a consistent order

	sim_db = get_similarity_dict(db, test_user_ids, SIM_FUNCTION)
	rec_db = predict_ratings(db, sim_db, movie_list, test_user_ids, N_TOP, K_NEIGHBORS)

	for test_user_id, rec_ary in rec_db.items():
		print(f'Recommended movies for user {test_user_id}:')
		print(rec_ary)
