import sys
import numpy as np

input_filename = sys.argv[1]
output_filename = sys.argv[2]

def get_eigen_vecs(matrix,n):
    eig_vals, eig_vecs = np.linalg.eig(matrix)
    real_eig_vals = np.real(eig_vals)
    real_eig_vecs = np.real(eig_vecs)
    index = real_eig_vals.argsort()[::-1]
    real_eig_vals = real_eig_vals[index]
    real_eig_vecs = real_eig_vecs[:,index]
    real_eig_vecs = np.reshape(real_eig_vecs,(n,n))

    return real_eig_vecs

def get_nodes(eig_vecs,n):
    eigen_vecs = eig_vecs[:,0]
    index = eigen_vecs.argsort()[::-1]
    nodes = np.array(list(range(n)))
    imp_nodes = nodes[index]
    imp_20 = imp_nodes[:20]
    
    return imp_20

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
adj = adj.T
deg = np.sum(adj,axis=0)
for i in range(len(deg)):
    if deg[i]!=0:
        adj[:,i] = adj[:,i]/deg[i]

K = 1/len(adj)
for i in range(len(adj)):
    if np.sum(adj[:,i]) == 0:
        adj[:,i] = K
N = np.ones((n,n))
N = K*N
b = 0.8
A = b*adj + (1-b)*N
real_eig_vecs = get_eigen_vecs(A,n)
important_nodes = get_nodes(real_eig_vecs,n)

file = open(output_filename,'w')
for i in important_nodes:
    file.write(str(i)+'\n')
file.close()