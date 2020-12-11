from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
import numpy as np


data_file = open("./smsspamcollection/SMSSpamCollection","r")
text_data =[]
label=[]

for line in data_file.readlines():
	data = line.split("\t")
	text_data.append(data[1])
	label.append(data[0])

#extract the countvectorizer features for the train data
count_vect = CountVectorizer()
X_train_counts = count_vect.fit_transform(text_data[:4400])


#train the classifier
clf = SGDClassifier(loss='hinge', penalty='l2',alpha=1e-2, random_state=42,max_iter=15, tol=None).fit(X_train_counts, label[:4400])

#do predictions on new text
docs_test = text_data[4400:]

#extract the features for this test set (new unseen text)
X_test_counts = count_vect.transform(docs_test)

#do the predictions
predicted = clf.predict(X_test_counts)

#print the accuracy
print("Accuracy on test set: ",np.mean(predicted == label[4400:]))