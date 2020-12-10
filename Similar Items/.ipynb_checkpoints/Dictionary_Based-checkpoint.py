import sys
import pdb
import time

def find_ngrams(input_list, n):
	return zip(*[input_list[i:] for i in range(n)])

def get_shingle_dict(n_grams, filename):
	#db mappings each shingle to an unique index. shingle->index
	db = {}
	shingles = [] #list of set

	for line in open(filename, 'r', encoding='utf-8'):
		tokens = [t for t in line[:-1].split(' ') if  t != '']

		shingle = set()
		for ngram in find_ngrams(tokens, n_grams):
			#frozenset to make it order independent and hashable
			ngram = frozenset(ngram)

			if ngram not in db:
				db[ngram] = len(db)

			shingle.add(db[ngram])
		shingles.append(shingle)

	return shingles, db

def find_similar_from_list(shingles, query_index, threshold, sort=False):
	sims = []
	query = shingles[query_index]

	for i, s in enumerate(shingles):
		sim = len(s & query) / len(s | query) # Jaccard Similarity
		if sim >= threshold:
			sims.append((i, sim))

	if sort:
		#sort by similarity in decreasing order
		sims = sorted(sims, key=lambda x: x[1], reverse=True)

	return sims

def print_similar_items(sims, query_index, threshold):
	print(f'Reviews similiar to review #{query_index} with similarity greater than {threshold}')
	for sim in sims:
		print(sim)

	print(f'Total: {len(sims)}')

#python3 Dictionary_Based.py sample.txt 467 5 0.4
if __name__ == '__main__':
	filename = sys.argv[1]
	query_index = int(sys.argv[2])
	n_grams = int(sys.argv[3])
	threshold = float(sys.argv[4])

	time_start = time.time()

	#shingles: a list of documents represented by shingle
	#db: map between shingle and its index
	shingles, db = get_shingle_dict(n_grams, filename)
	print('#Distinct Shingles:', len(db))

	sims = find_similar_from_list(shingles, query_index, threshold, True)
	print_similar_items(sims, query_index, threshold)

	time_end = time.time()
	elapsed = time_end - time_start
	print(f'Elapsed: {elapsed}')
