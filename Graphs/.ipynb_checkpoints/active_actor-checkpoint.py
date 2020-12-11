import networkx as nx

#load the graph made in previous task
G = nx.read_gexf("movie_graph.gexf")

#find the 5 actors that collaborated with the most actors
D = nx.degree_centrality(G)
L = sorted(D.items(),key=lambda item: item[1],reverse=True)
for i in range(5):
	print(L[i][0],":",L[i][1])

#degree of the most popular actor should be equal to its degree centrality*(number of nodes-1)
print("Degree of the most popular actor =", G.degree(L[0][0]))

print("Degree centrality times n_nodes-1 of the most popular actor =",L[0][1]*(G.number_of_nodes()-1))
