import re
from bs4 import BeautifulSoup
import html5lib
import lxml
from nltk.tokenize import WordPunctTokenizer


# run codes here on your target file
# if error shows telling you that there is a model version issue,
# you may need to run the file "sentiment_train_example.py" to update the model version.

# to get both sad and happy sentiments, you will need to run this code twice.
# see the lines where I indicate, switch model between "happy" and "sad".

# clean the tweets 
tok = WordPunctTokenizer()

pat1 = r'@[A-Za-z0-9_]+'
pat2 = r'https?://[^ ]+'
pat3 = r'\\'
pat4= r':\\'
emotion=r'[:;]+["^-]*[()]+'
combined_pat = r'|'.join((pat1, pat2, pat3, emotion))
www_pat = r'www.[^ ]+'
negations_dic = {"isn't":"is not", "aren't":"are not", "wasn't":"was not", "weren't":"were not",
                "haven't":"have not","hasn't":"has not","hadn't":"had not","won't":"will not",
                "wouldn't":"would not", "don't":"do not", "doesn't":"does not","didn't":"did not",
                "can't":"can not","couldn't":"could not","shouldn't":"should not","mightn't":"might not",
                "mustn't":"must not"}
neg_pattern = re.compile(r'\b(' + '|'.join(negations_dic.keys()) + r')\b')

def tweet_cleaner(text):
    
    neg_handled = neg_pattern.sub(lambda x: negations_dic[x.group()], text)
    soup = BeautifulSoup(neg_handled, 'html.parser')
    souped = soup.get_text()
    try:
        bom_removed = souped.encode('ascii', 'ignore').decode('utf-8-sig').replace(u"\ufffd", "?")
    except:
        bom_removed = souped
        
    stripped = re.sub(combined_pat, '', bom_removed)
    stripped = re.sub(www_pat, '', stripped)
    lower_case = stripped.lower()
    letters_only = re.sub("[^a-zA-Z]", " ", lower_case)
    # During the letters_only process two lines above, it has created unnecessay white spaces,
    # I will tokenize and join together to remove unneccessary white spaces
    words = [x for x  in tok.tokenize(letters_only) if len(x) > 1]
    return (" ".join(words)).strip()




import os
import glob
import pandas as pd
# from sklearn.externals import joblib
import joblib
from sklearn.pipeline import Pipeline


# pkl_path is the directory to save sentiment models
# we have both models to predict happy and sad,
# change "happy" to "sad" on where I indicate to change the model. 
pkl_path="/model_0.22.2.post1/"
m_file=pkl_path+'svm_happy.pkl' ############# sad model: m_file=pkl_path+'svm_sad.pkl'
vec_file=pkl_path+'svm_happy_vectorizer.pkl' ############### sad model: vec_file=pkl_path+'svm_sad_vectorizer.pkl'

model = joblib.load(m_file) 
vec = joblib.load(vec_file) 
text_clf = Pipeline([('vect', vec),
                     ('clf', model),
                    ])

# path is the directory that saves all the processed tweet files
# code will process all files that saved under this path
path="./"
path2 = "./"

for file in os.listdir(path):
    filepath=os.path.join(path, file)
    
    rawtweet=pd.read_csv(filepath, error_bad_lines=False, encoding='utf8') #,sep="\t"
    # encoding='utf8'

    cleaned_tweet=[]
    sad=[]
    
    for t in rawtweet['tweet_text']: #tweet_text
        
        cleaned_tweet.append(tweet_cleaner(t))
        
    rawtweet['tweet_id']=rawtweet['tweet_id']
    rawtweet['cleaned_tweet']=cleaned_tweet
    sad=text_clf.predict(cleaned_tweet)
    rawtweet['happy']=sad ################ dont forget to change csv variable name. sad model: rawtweet['sad']=sad
    
    # save output file
    rawtweet.to_csv(path2+ file + 'happy.csv') ############# dont forget to change csv name. sad model: rawtweet.to_csv(path2+ file + 'sad.csv')
    
    








