import json
import networkx as nx
import numpy as np
import random
from utils import create_graph

K = 10

def page_rank(G, d=0.85, tol=1e-2, max_iter=100, log=False):
    """Return the PageRank of the nodes in the graph. 

    :param dict G: the graph
    :param float d: the damping factor (teleportation)
    :param flat tol: tolerance to determine algorithm convergence
    :param int max_iter: max number of iterations
    """
    matrix = nx.to_numpy_matrix(G).T
    out_degree = matrix.sum(axis=0)
    N = G.number_of_nodes()
    # To avoid dead ends, since out_degree is 0, replace all the values with 1/N to make it column stochastic
    weight = np.divide(matrix, out_degree, out=np.ones_like(matrix)/N, where=out_degree!=0)
    pr = np.ones(N).reshape(N, 1) * 1./N

    for it in range(max_iter):
        old_pr = pr[:]
        pr = d * weight.dot(pr) + (1-d)/N
        if log:
            print (f'old_pr: {np.asarray(old_pr).squeeze()}, pr: {np.array(pr).squeeze()}')
        err = np.absolute(pr - old_pr).sum()
        if err < tol:
        	return pr

    return pr


if __name__ == '__main__':
    PATH = 'graph.json'

    g = create_graph(PATH)

    print ('Total nodes: ', g.number_of_nodes())
    print ('Total edges: ', g.number_of_edges())
    print()

    print('Pagerank scores from networkx')
    for node, score in sorted(nx.pagerank(g).items(), key=lambda x: x[1], reverse=True)[:K]:
        print (node, score)

    print()

    pr = page_rank(g)
    node_index_mapping = {i: node for i, node in enumerate(g.nodes())}
    most_important_nodes = sorted(enumerate(np.asarray(pr).squeeze()), reverse=True, key=lambda x: x[1])[:K]

    print('Pagerank score from vanilla implementation')
    for node_index, score in most_important_nodes:
        print (node_index_mapping[node_index], score)


