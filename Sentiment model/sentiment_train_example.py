# run the file "sentiment_prediction_example.py" first on your target file.
# if error shows telling you that there is a model version issue,
# you may need to run this file "sentiment_train_example.py" to update the model version.


#!/usr/bin/env python
# coding: utf-8


#classifier: SVM
#data used for training: sentiment140 mannual, sanders, kaggle, thu, qc



from time import time
import scipy
import numpy
import pandas as pd
import re
from sklearn.feature_extraction.text import CountVectorizer
import sys
import sklearn
import codecs
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import f1_score

from sklearn.model_selection import GridSearchCV
from sklearn.utils import shuffle
import numpy as np
import pickle
import builtins
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn import linear_model
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import Pipeline
from pprint import pprint
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
stop=stopwords.words('english')
import pandas as pd
import numpy as np
import os
import joblib

os.chdir('/')




#check scikit-learn version
#if new version is installed on the user's computer
#the user may need to update the code a little bit 
#since some functions may deprecate/replaced by new ones
import sklearn
sklearn.__version__


#######################################
###sad model start
#######################################

###read in data with labels from our group
thu=pd.read_csv('/training_data/thu.csv')


thu=thu.dropna(axis=0, subset=['text','target','sad_manual'])
thu['sad_manual']=thu['sad_manual'].astype(np.int64)
thutext=thu['text'].tolist()
thutarget=thu['target'].tolist()
thusad=thu['sad_manual'].tolist()

thutextarray=np.array(thutext)
thusadarray=np.array(thusad)
thutargetarray=np.array(thutarget)


train_no_1401=pd.read_csv('/training_data/train_no_1401.csv')

###define sadness: neutral and irrelevant are categorized as not sad(sad=0)
def definesad(train_no_1401):
    if train_no_1401['target']==0:
        return 1
    else:
        return 0
        
train_no_1401['sad']=train_no_1401.apply(lambda train_no_1401: definesad(train_no_1401), axis=1)
train_no_1401['sad']=train_no_1401['sad'].astype(np.int64)

train_no_1401=train_no_1401.dropna(axis=0, subset=['text','sad'])

train_sad_text=train_no_1401['text'].tolist()+thutext
train_sad_y=train_no_1401['sad'].tolist()+thusad
sadtextarray=np.array(train_sad_text)
sadyarray=np.array(train_sad_y)



### train sad model
#model selection is performed elsewhere
#after comparison, we decided to use linear SVM
#the choice of parameters within SVM does not affect the result too much

import joblib
from sklearn import svm

tvec = TfidfVectorizer(stop_words=stop)
model = svm.SVC(kernel='linear', C=1)

pipe = Pipeline([
    ('vectorizer', tvec),
    ('classifier', model)
])

sentiment_fit=pipe.fit(sadtextarray,sadyarray)


#dump model to pkl and saved 
#next time the user just need to read in the models and then perform prediction
joblib.dump(model, '/model_0.22.2.post1/svm_sad.pkl') 
joblib.dump(tvec, '/model_0.22.2.post1/svm_sad_vectorizer.pkl') 

#######################################
###sad model end
#######################################

# test checking
s = ['I love u', 'i hate u','I dont like vegetable']
sentiment_fit.predict(s)





#######################################
###happy model start
#######################################
#first read in happy tweets 
qc=pd.read_csv('/training_data/qc.csv')
qc=qc.dropna(axis=0, how='any')


def definehappy(train_no_1401):
    if train_no_1401['target']==1:
        return 1
    else:
        return 0
        
train_no_1401['happy']=train_no_1401.apply(lambda train_no_1401: definehappy(train_no_1401), axis=1)
qc['happy']=qc.apply(lambda qc: definehappy(qc), axis=1)
thu['happy']=thu.apply(lambda thu: definehappy(thu), axis=1)

train_happy_text=train_no_1401['text'].tolist()+thu['text'].tolist()+qc['clean_text'].tolist()
train_happy_y=train_no_1401['happy'].tolist()+thu['happy'].tolist()+qc['happy'].tolist()

happytextarray=np.array(train_happy_text)
happyyarray=np.array(train_happy_y)


#train happy model
#from sklearn.externals import joblib

#import joblib
#from sklearn import svm

tvec = TfidfVectorizer(stop_words=stop)
model = svm.SVC(kernel='linear', C=1)

pipe = Pipeline([
    ('vectorizer', tvec),
    ('classifier', model)
])
happy_fit=pipe.fit(happytextarray,happyyarray)


#from sklearn.externals import joblib
joblib.dump(model, '/model_0.22.2.post1/svm_happy.pkl') 
joblib.dump(tvec, '/model_0.22.2.post1/svm_happy_vectorizer.pkl') 


#######################################
###happy model end
#######################################


#test checking
s = ['I love u', 'i hate u','I dont like vegetable']
happy_fit.predict(s)

