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

mycursor = mydb.cursor()
selectQuery = "SELECT article FROM bios_text LIMIT 1000"
mycursor.execute(selectQuery)
myresult = mycursor.fetchall()

#1 add strip wikitext
def lemmatize_stemming(text):
    return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))
def preprocess(text):
    result = []
    for token in gensim.utils.simple_preprocess(text):
        if token not in gensim.parsing.preprocessing.STOPWORDS and len(token) > 3:
            result.append(lemmatize_stemming(token))
    return result

# print(myresult[0][0])
processed_docs=[]
#PREPROCESS ARTICLES9
for result in myresult:
    processed_docs.append(preprocess(result[0]))
# processed_docs = map(preprocess, myresult)

with open("processed_docs.txt", "w") as fp:
    json.dump(processed_docs, fp)
print("Preprocessed docs")
# print(processed_docs)
# #
# dictionary = gensim.corpora.Dictionary(processed_docs)
# # print(dictionary)
#
# dictionary.save("dict")
# dict = gensim.corpora.Dictionary.load("dict")
# print(dict)
# count = 0
# for k, v in dictionary.iteritems():
#     print(k, v)
#     count += 1
#     if count > 10:
#         break
# # #2
# with open('my_dict.json', 'w') as f:
#     json.dump(dictionary, f)
