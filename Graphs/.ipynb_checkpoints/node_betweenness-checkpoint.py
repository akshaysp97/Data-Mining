import networkx as nx
import matplotlib.pyplot as plt

G = nx.Graph()        

G.add_edge('a', 'b')
G.add_edge('a', 'c')
G.add_edge('b', 'c')
G.add_edge('c', 'd')
G.add_edge('d', 'e')
G.add_edge('d', 'f')
G.add_edge('e', 'f')
.
nx.draw(G,with_labels=True)
plt.show()

#calculate the betweenness centrality for nodes.
print(nx.betweenness_centrality(G,normalized=False))