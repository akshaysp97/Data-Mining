from sklearn.datasets import fetch_20newsgroups
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import SGDClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np

#load the news dataset
categories = ['alt.atheism', 'soc.religion.christian','comp.graphics', 'sci.med']
twenty_train = fetch_20newsgroups(subset='train',categories=categories, shuffle=True, random_state=42)

#extract the countvectorizer and tfidf features for the train data 
vectorizer = TfidfVectorizer()
X_train_tfidf = vectorizer.fit_transform(twenty_train.data)

#train the classifier using the tfidf features
clf = SGDClassifier(loss='hinge', penalty='l2',alpha=1e-4, random_state=42,max_iter=45, tol=None).fit(X_train_tfidf, twenty_train.target)

twenty_test = fetch_20newsgroups(subset='test',categories=categories, shuffle=True, random_state=42)

test_tfidf = vectorizer.transform(twenty_test.data)

predicted = clf.predict(test_tfidf)

#print the accuracy on the test set
print("Accuracy on test set: ",np.mean(predicted == twenty_test.target))