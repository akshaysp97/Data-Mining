import networkx as nx
import matplotlib.pyplot as plt

G = nx.Graph()        

G.add_edge('a', 'b')
G.add_edge('a', 'e')
G.add_edge('b', 'f')
G.add_edge('b', 'g')
G.add_edge('e', 'f')
G.add_edge('f', 'd')
G.add_edge('d', 'g')

nx.draw(G,with_labels=True)
plt.show()

#calculate the betweenness centrality for nodes.
print(nx.edge_betweenness_centrality(G,normalized=False))