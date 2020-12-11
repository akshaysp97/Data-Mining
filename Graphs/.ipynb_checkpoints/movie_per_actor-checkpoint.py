import networkx as nx

#load the graph made in previous task
G = nx.read_gexf("movie_graph.gexf")

print("Number of nodes in this multigraph =", G.number_of_nodes())
print("Number of edges in this multigraph =", G.number_of_edges())

D = {}
for v in G.nodes():
	#for each node (actor) find all the edges associated with it
    E = list(G.edges(v, data = True))
    S = set()
    #iterate over the edges and extract the movie titles
    for e in E:
        S.add(e[2]["title"])
    #add the coresponding set of movies to the dictionary coresponding to the actor key
    D[v] = S
#sort and get the top 5 
L = sorted(D.items(), key=lambda item: len(item[1]), reverse=True)
for i in range(5):
    print(L[i][0], ":", len(L[i][1])) 