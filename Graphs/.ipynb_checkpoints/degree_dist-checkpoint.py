import collections
import matplotlib.pyplot as plt
import networkx as nx
import json

def network():
	#create an undirected graph similar to the multigraph we created before
	Movies = []
	with open("small_data.json", "r", encoding="utf-8") as f:
	    for line in f.readlines():
	        J = json.loads(line)
	        Movies.append(J)


	G = nx.Graph()        
	for movie in Movies:
	    for i in range(0, len(movie["cast"]) - 1):
	        for j in range(i + 1, len(movie["cast"])):
	            G.add_edge(movie["cast"][i], movie["cast"][j], title=movie["title"]) 
	return G  


def bar_plot(G):
	#degree distribtuion bar plot
	degree_sequence = sorted([d for n, d in G.degree()])  
	degreeCount = collections.Counter(degree_sequence)
	deg, cnt = zip(*degreeCount.items())
	cnt = [x / nx.number_of_nodes( G) for x in cnt]

	fig, ax = plt.subplots()

	plt.bar(deg, cnt, width=0.80, color="b")
	plt.title("Degree Histogram")
	plt.ylabel("Fraction of Nodes")
	plt.xlabel("Degree")
	plt.show()

def log_scale(G):
	#degree distribution in log scale 
	degree_sequence = sorted([d for n, d in G.degree()])  
	degreeCount = collections.Counter(degree_sequence)
	deg, cnt = zip(*degreeCount.items())
	cnt = [x / nx.number_of_nodes( G) for x in cnt]

	fig, ax = plt.subplots()

	plt.plot(deg, cnt, 'o')
	plt.xlabel('Degree')
	plt.ylabel('Fraction of Nodes')
	plt.xscale('log')
	plt.yscale('log')
	plt.show()


if __name__ == "__main__":
	G = network()
	bar_plot(G)
	log_scale(G)