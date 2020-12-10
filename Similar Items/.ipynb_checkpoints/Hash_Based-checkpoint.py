import sys
import pdb
import time

from Dictionary_Based import find_ngrams, print_similar_items, find_similar_from_list

def get_shingle_hash(n_grams, hash_size, filename):
	shingles = [] #list of set

	for line in open(filename, 'r', encoding='utf-8'):
		tokens = [t for t in line[:-1].split(' ') if  t != '']

		shingle = set()
		for ngram in find_ngrams(tokens, n_grams):
			ngram = frozenset(ngram)
			ngram_hash = hash(ngram) % hash_size

			shingle.add(ngram_hash)
		shingles.append(shingle)

	return shingles, None

#python3 Hash_Based.py sample.txt 467 5 0.4
if __name__ == '__main__':
	filename = sys.argv[1]
	query_index = int(sys.argv[2])
	n_grams = int(sys.argv[3])
	threshold = float(sys.argv[4])

	time_start = time.time()

	hash_size = 2**20

	shingles, _ = get_shingle_hash(n_grams, hash_size, filename)
	sims = find_similar_from_list(shingles, query_index, threshold, True)

	print_similar_items(sims, query_index, threshold)

	time_end = time.time()
	elapsed = time_end - time_start
	print(f'Elapsed: {elapsed}')
