from sklearn.datasets import fetch_20newsgroups
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
import numpy as np

#load the news dataset
categories = ['alt.atheism', 'soc.religion.christian','comp.graphics', 'sci.med']
twenty_train = fetch_20newsgroups(subset='train',categories=categories, shuffle=True, random_state=42)

#extract the countvectorizer features for the train data
count_vect = CountVectorizer()
X_train_counts = count_vect.fit_transform(twenty_train.data)


#train a multinomial naive Bayes classifier
clf = MultinomialNB().fit(X_train_counts, twenty_train.target)


#do predictions on new text
docs_new = ['God is love', 'OpenGL on the GPU is fast']

#extract the features for this test set (new unseen text)
X_new_counts = count_vect.transform(docs_new)

#do the predictions
predicted = clf.predict(X_new_counts)

#print out the predictions
for doc, category in zip(docs_new, predicted):
	print('%r => %s' % (doc, twenty_train.target_names[category]))

twenty_test = fetch_20newsgroups(subset='test',categories=categories, shuffle=True, random_state=42)
docs_test = count_vect.transform(twenty_test.data)
predicted = clf.predict(docs_test)
#print the accuracy on the test set
print("Accuracy on test set: ",np.mean(predicted == twenty_test.target))
