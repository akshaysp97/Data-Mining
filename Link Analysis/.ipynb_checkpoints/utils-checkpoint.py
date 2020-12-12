import networkx as nx
import json

def create_graph(path):
	"""Generate a networkx graph from specified path. 

    :param dict path: the graph
    """
	g = nx.DiGraph()

	for line in open(path):
		d = json.loads(line)
		url = d['url']
		g.add_node(url)
		for linked_url in d['linked_urls']:
			g.add_node(linked_url)
			g.add_edge(url, linked_url)

	return g
