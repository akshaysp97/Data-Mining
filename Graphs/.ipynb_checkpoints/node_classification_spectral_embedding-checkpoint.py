import sys
import numpy as np
import csv
from sklearn.neighbors import KNeighborsClassifier

input_filename = sys.argv[1]
label_train_filename = sys.argv[2]
label_test_filename = sys.argv[3]
output_filename = sys.argv[4]

def read_label(file):
    with open(file) as f:
        read = csv.reader(f, delimiter=" ")
        data = [row for row in read]
    data = np.array(data)
    
    return data

def get_embedding(nodes,eigen_vecs):
    lst = []
    for i in nodes:
        lst.append(eigen_vecs[int(i[0])])
    return lst

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

node_embedding = []
count = 0
for v,eig in zip(real_eig_vals, real_eig_vecs.T):
    if v > 0.00001 and count <= 60:
        node_embedding.append(eig)
        count+=1                      
eigen_vecs = np.reshape(node_embedding,(count,n))
eigen_vecs = eigen_vecs.T

training_data = read_label(label_train_filename)
training_nodes = training_data[:,[0]]
training_labels = training_data[:,1]
embedd_train = get_embedding(training_nodes,eigen_vecs)
embedd_train = np.reshape(embedd_train,(len(training_nodes),count))

testing_data = read_label(label_test_filename)
testing_nodes = testing_data[:,[0]]
embedd_test = get_embedding(testing_nodes,eigen_vecs)
embedd_test = np.reshape(embedd_test,(len(testing_nodes),count))

knn = KNeighborsClassifier(n_neighbors=6)
knn.fit(embedd_train, training_labels)
testing_labels = knn.predict(embedd_test)

f = open(output_filename,'w')
for i,j in zip(testing_nodes,testing_labels):
    node = i[0]
    f.write(str(node)+' '+str(j)+'\n')
f.close()