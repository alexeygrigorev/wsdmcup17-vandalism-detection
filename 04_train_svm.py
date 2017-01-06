# coding: utf-8

from time import time
import cPickle 

import numpy as np
import scipy.sparse as sp

import feather
from tqdm import tqdm

from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.svm import LinearSVC
from sklearn.metrics import roc_auc_score


# reading the data

df = feather.read_dataframe('df_features.feather')

all_text = 'title=' + df.page_title + ' ' + \
            df.user_info + ' ' + \
            df.comment_structured_text + ' ' + \
            df.comment_links + ' ' + \
            df.comment_unstructured_text

fold = df.fold

# label data 

y_train = df[fold == 0].reverted.values
y_val = df[fold == 1].reverted.values
y_test = df[fold == 2].reverted.values


# feature matrices

text_vec = HashingVectorizer(dtype=np.uint8, n_features=10000000,
    norm=None, lowercase=False, binary=True, token_pattern='\\S+', 
    non_negative=True)

X_train = text_vec.transform(all_text[fold == 0])
X_val = text_vec.transform(all_text[fold == 1])
X_test = text_vec.transform(all_text[fold == 2])


# model: grid search

for C in [0.001, 0.01, 0.1, 0.5, 1, 5]:
    t0 = time()

    svm = LinearSVC(penalty='l1', dual=False, C=C, random_state=1)
    svm.fit(X_train, y_train)

    y_pred = svm.decision_function(X_val)
    auc = roc_auc_score(y_val, y_pred)

    preds[C] = y_pred
    aucs[C] = auc
    models[C] = svm

    print 'C=%s, took %.3fs, auc=%.3f' % (C, time() - t0, auc)


# model: testing

C = 0.5

X = sp.vstack([X_train, X_val])
y = np.concatenate([y_train, y_val])


t0 = time()

svm = LinearSVC(penalty='l1', dual=False, C=C, random_state=1)
svm.fit(X, y)

y_pred = svm.decision_function(X_test)
auc = roc_auc_score(y_test, y_pred)

print 'best model: C=%s, took %.3fs, test auc %.3f' % (C, time() - t0, auc)



# final model training

X = sp.vstack([X_train, X_val, X_test])
y = np.concatenate([y_train, y_val, y_test])

t0 = time()

svm = LinearSVC(penalty='l1', dual=False, C=C, random_state=1)
svm.fit(X, y)

print 'final model training took %.3fs' % (time() - t0)


# saving the models

print 'saving the models...'

with open('model_vect.bin', 'wb') as f:
    cPickle.dump(text_vec, f)

with open('model_svm.bin', 'wb') as f:
    cPickle.dump(svm, f)

