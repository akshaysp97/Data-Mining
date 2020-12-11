import json
import networkx as nx
import sys

input_filename = sys.argv[1]
gexf_output_filename = sys.argv[2]
output_filename = sys.argv[3]

tweets = []
with open(input_filename, "r", encoding="utf-8") as f:
    for line in f.readlines():
        J = json.loads(line)
        tweets.append(J)

G = nx.DiGraph()
for item in tweets:
    X = item["user"]["screen_name"]
    G.add_node(X)
    if "retweeted_status" in item:
        Y = item["retweeted_status"]["user"]["screen_name"]
        if G.has_edge(X,Y):
            G[X][Y]['weight']+=1
        else:
            weight = 1
            G.add_weighted_edges_from([(X,Y,weight)])

nx.write_gexf(G, gexf_output_filename)

D = {}
K = {}
for v in G.nodes():
    E = list(G.edges(v, data = True))
    count=0
    for e in E:
        n = e[1]
        wt = e[2]['weight']
        if n not in K:
            K[n] = wt
        else:
            K[n] = K.get(n,0)+wt
            
        if wt == 0:
            continue
        else:
            n = (wt)
            count+=n
        D[v] = count
   

maximum = max(D, key=D.get) 
maximum_1 = max(K, key=K.get)


output = {"n_nodes": G.number_of_nodes() , "n_edges": G.number_of_edges(), "max_retweeted_user": maximum_1,"max_retweeted_number": K[maximum_1],"max_retweeter_user": maximum, "max_retweeter_number": D[maximum]}

with open(output_filename,"w") as json_file:
    json.dump(output, json_file)