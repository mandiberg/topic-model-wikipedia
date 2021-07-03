import json
import csv
import numpy as np
import nltk
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from gensim import corpora, models
import mysql.connector
import databaseconfig as cfg
import gensim
from datetime import date
today = date.today()
d4 = today.strftime("%b-%d-%Y")


np.random.seed(2018)

nltk.download('wordnet')
stemmer = SnowballStemmer("english")

mydb = mysql.connector.connect (
    host= cfg.mysql["host"],
    user=cfg.mysql["user"],
    password=cfg.mysql["password"],
    database=cfg.mysql["database"]
)
# number of records you want to preprocess
number_of_records = '500000'
our_stopwords = []

mycursor = mydb.cursor()
selectQuery = "SELECT id, article, enwiki_title FROM bios_final LIMIT "+number_of_records
mycursor.execute(selectQuery)
myresult = mycursor.fetchall()

#1 add strip wikitext
def lemmatize_stemming(text):
    return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))
def preprocess(text, stops):
    result = []
    for token in gensim.utils.simple_preprocess(text):
        if token not in gensim.parsing.preprocessing.STOPWORDS and lemmatize_stemming(token) not in stops and len(token) > 3:
            result.append(lemmatize_stemming(token))
    return result

with open('stopwords.csv') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        gensim.utils.simple_preprocess(row[0])
        for sw in gensim.utils.simple_preprocess(row[0]):
            our_stopwords.append(lemmatize_stemming(sw))


processed_docs=[]
#PREPROCESS ARTICLES9
for result in myresult:
    stop_name = []
    for name in result[2].split("_"):
        pr_name = gensim.utils.simple_preprocess(name)
        for n in pr_name:
            stop_name.append(lemmatize_stemming(n))

    prprocessed = preprocess(result[1], our_stopwords+stop_name)
    processed_docs.append(prprocessed)

with open("processed_docs_"+number_of_records+"_"+d4+".txt", "w") as fp:
    json.dump(processed_docs, fp)
print("Preprocessed docs")
# print(processed_docs)
# #
dictionary = gensim.corpora.Dictionary(processed_docs)
# print(dictionary)
#
dictionary.save("dict_"+number_of_records+"_"+d4)
# dict = gensim.corpora.Dictionary.load("dict")
# print(dict)
# count = 0
# for k, v in dictionary.iteritems():
#     print(k, v)
#     count += 1
#     if count > 10:
#         break
