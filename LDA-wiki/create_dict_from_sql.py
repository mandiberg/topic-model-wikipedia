import json
import numpy as np
import nltk
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from gensim import corpora, models
import mysql.connector
import databaseconfig as cfg
import gensim

np.random.seed(2018)

nltk.download('wordnet')
stemmer = SnowballStemmer("english")

mydb = mysql.connector.connect (
    host= cfg.mysql["host"],
    user=cfg.mysql["user"],
    password=cfg.mysql["password"],
    database=cfg.mysql["database"]
)
our_stopwords = ["rowspan", "colspan", "efefef", "bgcolor", "afeee", "Jones", "New"]
mycursor = mydb.cursor()
selectQuery = "SELECT id, article FROM bios_final LIMIT 160000"
mycursor.execute(selectQuery)
myresult = mycursor.fetchall()

#1 add strip wikitext
def lemmatize_stemming(text):
    return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))
def preprocess(text):
    result = []
    for token in gensim.utils.simple_preprocess(text):
        if token not in gensim.parsing.preprocessing.STOPWORDS and token not in our_stopwords and len(token) > 3:
            result.append(lemmatize_stemming(token))
    return result

processed_docs=[]
#PREPROCESS ARTICLES9
for result in myresult:
    processed_docs.append(preprocess(result[1]))


with open("processed_docs_160000.txt", "w") as fp:
    json.dump(processed_docs, fp)
print("Preprocessed docs")
# print(processed_docs)
# #
dictionary = gensim.corpora.Dictionary(processed_docs)
# print(dictionary)
#
dictionary.save("dict_160000")
# dict = gensim.corpora.Dictionary.load("dict")
# print(dict)
# count = 0
# for k, v in dictionary.iteritems():
#     print(k, v)
#     count += 1
#     if count > 10:
#         break
