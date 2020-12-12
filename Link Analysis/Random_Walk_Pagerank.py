import json
import networkx as nx
import numpy as np
import random
from utils import create_graph

K = 10

def random_walk(G):
	"""
	Return the page rank of the graph G using random walk

	:param dict G: the graph
	"""
	nodes = list(G.nodes())
	K = 1000000
	curr = random.choice(nodes)
	visit = {curr: 1}
	out_edges = G.out_edges(curr)
	for _ in range(K):
		if len(out_edges) == 0:
			curr = random.choice(nodes)
		else:
			curr = random.choice(list(out_edges))[1]
		out_edges = G.out_edges(curr)
		visit[curr] = visit.get(curr, 0) + 1
	return visit

if __name__ == '__main__':
	PATH = 'graph.json'
	g = create_graph(PATH)

	rw = random_walk(g)
	for node, visit in sorted(rw.items(), key=lambda x: x[1], reverse=True)[:K]:
		print (node, visit)