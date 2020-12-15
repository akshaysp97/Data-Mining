import sys
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics.cluster import adjusted_rand_score

input_filename = sys.argv[1]
output_filename = sys.argv[2]
k = int(sys.argv[3])

data = np.loadtxt(input_filename, dtype=int)
nodes = set()
data_0 = data[:,0]
data_1 = data[:,1]
for i in data_0:
    nodes.add(i)
for j in data_1:
    nodes.add(j)
n = len(nodes)
adj = np.zeros((n, n))
degree = np.zeros((n,n))
for i in data:
    node1 = i[0]
    node2 = i[1]
    if node1 != node2:
        adj[node1,node2] = 1
        adj[node2,node1] = 1

degree = np.diag(np.sum(adj,axis=1))
laplacian = np.zeros((n, n))
laplacian = degree - adj

eig_vals, eig_vecs = np.linalg.eig(np.matrix(laplacian))
eig_vals_sorted = np.sort(eig_vals)
eig_vecs_sorted = eig_vecs[:, eig_vals.argsort()]
real_eig_vals = np.real(eig_vals_sorted)
real_eig_vecs = np.real(np.array(eig_vecs_sorted))

node_embedding = np.zeros((n, n))
idx,count = 0,0
for v in eig_vals_sorted:
    if v > 0.00001 and count < 3:
        for i in range(0,n):
            node_embedding[i][count] = real_eig_vecs[i][idx]
        count+=1
    idx+=1

kmeans = KMeans(n_clusters=k)
kmeans.fit(node_embedding)
labels = kmeans.labels_
ns = np.array(list(range(n)))

f = open(output_filename,'w')
for i,j in zip(ns,labels):
    f.write(str(i)+' '+str(j)+'\n')
f.close()