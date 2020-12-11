import json
import networkx as nx
from collections import defaultdict
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
import sys

def retweet_nw(filename):
    tweets = []
    with open(filename, "r", encoding="utf-8") as f:
        for line in f.readlines():
            J = json.loads(line)
            tweets.append(J)
            
    return tweets

def build_graph(tweets):
    G = nx.Graph()
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

    return G

def weighted_edges(G):
    n = G.number_of_nodes()    
    A = nx.to_numpy_matrix(G)
    total_sum = 0    
    total_sum = A.sum()
    total_sum = total_sum/2.0
    
    return total_sum

def Degree_node(A, nodes):
    node_degree = {} 
    deg_mat = A.sum(axis = 1)
    count = 0
    for node in list(nodes):
        node_degree[node] = deg_mat[count, 0]
        count += 1
    return node_degree

def Girvan_Newman(G):
    edge_btwn = nx.edge_betweenness_centrality(G, weight='weight')
    mx_tuple = max(edge_btwn.items(),key = lambda x:x[1])
    max_list =[i[0] for i in edge_btwn.items() if i[1]==mx_tuple[1]]
    G.remove_edges_from(max_list)  

def Modularity_Split(G, degree_dict, m):
    New_A = nx.adj_matrix(G)
    New_deg = {}
    New_deg = Degree_node(New_A, G.nodes())
    components = nx.connected_components(G)   
    Mod = 0    
    for comp in components:
        Aij = 0    
        val = 0    
        for user in comp:
            Aij += New_deg[user]
            val += degree_dict[user]        
        Mod += ((Aij) - (val*val)/(2*m))
    Mod = Mod/(2*m)
    return Mod

def Community_Detection(G, degree_dict, m):
    modularity = 0
    max_modularity = 0
    while G.number_of_edges() != 0:  
        Girvan_Newman(G)
        modularity = Modularity_Split(G, degree_dict, m);
        if modularity > max_modularity:
            max_modularity = modularity
            best_comm = list(nx.connected_components(G))
  
        
    return best_comm, max_modularity

if __name__ == "__main__":
 
    input_filename = sys.argv[1]
    output_taskA = sys.argv[2]
    output_taskB = sys.argv[3]
    output_taskC = sys.argv[4]
    tweets=[]
    tweets= retweet_nw(input_filename)
    G = build_graph(tweets)
    A = nx.adj_matrix(G)    
    wt = weighted_edges(G)
    degree_dict = {}
    degree_dict = Degree_node(A, G.nodes())
    best_comm, Q = Community_Detection(G, degree_dict, wt)
    
    for i in range(len(best_comm)):
        best_comm[i] = sorted(best_comm[i])
    best_comm.sort()
    best_comm.sort(key=len)

    for i in range(len(best_comm)):
        best_comm[i] = list(best_comm[i])

    with open(output_taskA, 'w') as f:
        f.write('Best Modularity is: '+ str(Q) + '\n')
        for comm in best_comm:
            if len(comm)>1:
                com1 = comm[:-1]
                l = comm[-1]
                for item in com1:
                    f.write(repr(item)+r",")
                f.write(repr(l))
                f.write("\n")
            else:
                com1 = comm
                for item in com1:
                    f.write(repr(item))
                f.write("\n")
    f.close()

    lst=[]
    for j in range(1,3):
        lst.append(list(best_comm[-j]))

    tweet_text=defaultdict()
    for i in range(len(lst)):
        for j in range(len(lst[i])):
            user = lst[i][j]
            for item in tweets:
                if str(user) == item["user"]["screen_name"]:
                    if str(user) not in tweet_text:
                        tweet_text[str(user)]=item["text"]
                    else:
                        tweet_text[str(user)]+= (' ')
                        tweet_text[str(user)]+= (item["text"])
                        
                if "retweeted_status" in item:
                    if str(user) == item["retweeted_status"]["user"]["screen_name"]:
                        if str(user) not in tweet_text:
                            tweet_text[str(user)] = item["retweeted_status"]["text"]
                        else:
                            tweet_text[str(user)]+= (' ')
                            tweet_text[str(user)]+= (item["retweeted_status"]["text"])

    label_0 = {k: tweet_text[k] for k in list(tweet_text)[:len(lst[0])]}
    label_1 = {k: tweet_text[k] for k in list(reversed(list(tweet_text)))[:len(lst[1])]}

    data_0=[]
    user_0 = []
    user_1 = []
    for k,v in label_0.items():
        user_0.append(k)
        data_0.append(v)
            
    data_1 = []
    for k,v in label_1.items():
        user_1.append(k)
        data_1.append(v)

    data = data_0+data_1

    target = ([0]*len(data_0) + [1]*len(data_1))

    vectorizer = TfidfVectorizer()
    X_train_tfidf = vectorizer.fit_transform(data)

    clf = MultinomialNB().fit(X_train_tfidf, target)

    test = []
    for i in range(len(best_comm)-2):
        test.append(list(best_comm[i]))

    tweet_text_test=defaultdict(set)
    for i in range(len(test)):
        for j in range(len(test[i])):
            user_test = test[i][j]
            for item in tweets:
                if str(user_test) == item["user"]["screen_name"]:
                    if str(user_test) not in tweet_text_test:
                        tweet_text_test[str(user_test)]=item["text"]
                    else:
                        tweet_text_test[str(user_test)]+= (' ')
                        tweet_text_test[str(user_test)]+= (item["text"])
                if "retweeted_status" in item:
                    if str(user_test) == item["retweeted_status"]["user"]["screen_name"]:
                        if str(user_test) not in tweet_text_test:
                            tweet_text_test[str(user_test)] = item["retweeted_status"]["text"]
                        else:
                            tweet_text_test[str(user_test)]+= (' ')
                            tweet_text_test[str(user_test)]+= (item["retweeted_status"]["text"])

    data_test = []
    user_list = []
    for k, v in tweet_text_test.items():
        user_list.append(k)
        data_test.append(v)

    test_tfidf = vectorizer.transform(data_test)

    predicted = clf.predict(test_tfidf)

    predictions=[]
    predictions0 = []
    predictions1 = []
    for i in range(len(user_list)):
        predictions = [[user_list[i]]]
        predictions[0].append(predicted[i])
        if predictions[0][1] == 0:
            predictions0.append(str(predictions[0][0]))
        else:
            predictions1.append(str(predictions[0][0]))

    community1 = user_0+predictions0

    community1.sort()

    community2 = user_1+predictions1

    community2.sort()

    com1 = community1[:-1]
    l1 = community1[-1]
    com2 = community2[:-1]
    l2 = community2[-1]
    with open(output_taskB, 'w') as f:
        for item in com1:
            f.write(repr(item)+r",")
        f.write(repr(l1))
        f.write("\n")

        for item in com2:
            f.write(repr(item)+r",")
        f.write(repr(l2))

    f.close()

    count_vect = CountVectorizer()

    X_train_counts = count_vect.fit_transform(data)

    clf = MultinomialNB().fit(X_train_counts,target)

    docs_test = count_vect.transform(data_test)

    predicted_countvec = clf.predict(docs_test)

    predictions=[]
    predictions_countvec0 = []
    predictions_countvec1 = []
    for i in range(len(user_list)):
        predictions = [[user_list[i]]]
        predictions[0].append(predicted_countvec[i])
        if predictions[0][1] == 0:
            predictions_countvec0.append(str(predictions[0][0]))
        else:
            predictions_countvec1.append(str(predictions[0][0]))

    community1_countvec = user_0+predictions_countvec0

    community1_countvec.sort()

    community2_countvec = user_1+predictions_countvec1

    community2_countvec.sort()


    com1_countvec = community1_countvec[:-1]
    l1_countvec = community1_countvec[-1]
    com2_countvec = community2_countvec[:-1]
    l2_countvec = community2_countvec[-1]
    with open(output_taskC, 'w') as f:
        for item in com1_countvec:
            f.write(repr(item)+r",")
        f.write(repr(l1_countvec))
        f.write("\n")

        for item in com2_countvec:
            f.write(repr(item)+r",")
        f.write(repr(l2_countvec))

    f.close()


