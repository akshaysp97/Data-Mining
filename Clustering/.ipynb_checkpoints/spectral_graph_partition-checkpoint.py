import numpy as np
import sys
import json
from sklearn.neighbors import kneighbors_graph
import matplotlib.pyplot as plt

input_filename = sys.argv[1]
output_filename = sys.argv[2]

with open(input_filename) as f:
  data = json.load(f)

Adj = kneighbors_graph(data, 20, mode='connectivity', include_self=False)
Adj = Adj.toarray()
n = len(Adj)
for i in range(n):
    for j in range(n):
        if Adj[i][j]!=0:
            Adj[j][i] = Adj[i][j]

degree_matrix = np.diag(np.sum(Adj, axis=1))
laplacian_matrix = degree_matrix - Adj

eig_vals, eig_vecs = np.linalg.eig(np.matrix(laplacian_matrix))
eig_vals_sorted = np.sort(eig_vals)
eig_vecs_sort = eig_vecs[:, eig_vals.argsort()]
real_eig_vals = np.real(eig_vals_sorted)
real_eig_vecs = np.real(np.array(eig_vecs_sort))
mean = np.mean(real_eig_vecs)

outer= []
inner = []
for i in range(len(real_eig_vecs)):
    point = real_eig_vecs[i][1]
    if point > mean:
        outer.append(data[i])
    else:
        inner.append(data[i])
        
outer = np.array(outer)
inner = np.array(inner)

plt.scatter(outer[:,0], outer[:,1],color = 'yellow')
plt.scatter(inner[:,0], inner[:,1], color = 'purple')
plt.savefig(output_filename)