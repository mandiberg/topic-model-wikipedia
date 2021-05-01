import json
import mysql.connector
import numpy as np
import nltk
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from gensim import corpora, models
from wikitextparser import remove_markup, parse
import databaseconfig as cfg

mydb = mysql.connector.connect (
    host= cfg.mysql["host"],
    user=cfg.mysql["user"],
    password=cfg.mysql["password"],
    database=cfg.mysql["database"]
)

mycursor = mydb.cursor()
mycursor.execute("CREATE TABLE bios_text (id VARCHAR(20),enwiki_title VARCHAR(500),occupation VARCHAR(200),gender VARCHAR(100),citizenship VARCHAR(200), article longtext)")

# np.random.seed(2018)
nltk.download('wordnet')
stemmer = SnowballStemmer("english")
#1 add strip wikitext
def lemmatize_stemming(text):
    return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))
def preprocess(text):
    result = []
    for token in gensim.utils.simple_preprocess(text):
        if token not in gensim.parsing.preprocessing.STOPWORDS and len(token) > 3:
            result.append(lemmatize_stemming(token))
    return result


insertQuery = "SELECT article, id FROM bios WHERE article is not NULL"
mycursor.execute(insertQuery)
# mydb.commit()
myresult = mycursor.fetchall()
# x = 0
# while x < 3:
#     print(myresult[x][0])
#     x=x+1
plain_results = []
for result in myresult:
    if result is not None:
        # print(result[0])
        # print(result[1])
        # print(type(result[0]))
        try:
            plain=remove_markup(result[0])
            plain_results.append(plain)
            # print(plain)
            # mycursor.execute(f"UPDATE bios SET article='{plain}' where id='{result[1]}'")
            # mydb.commit()


        except:
            mycursor.execute(f"UPDATE bios SET Err = TRUE where id='{result[1]}'")
            mydb.commit()
            print(result)
# print(plain_results[:5])
print("Finished removing wikitext markup.")
# print(len(documents))
# print(documents[:5])

# doc_sample = documents[documents['index'] == 4310].values[0][0]
# print('original document: ')

processed_docs = map(preprocess, plain_results)
print("Preprocessed docs: (1)")
print(processed_docs[:1])
#
dictionary = gensim.corpora.Dictionary(processed_docs)
count = 0
for k, v in dictionary.iteritems():
    print(k, v)
    count += 1
    if count > 10:
        break
# #2
with open('my_dict.json', 'w') as f:
    json.dump(dictionary, f)
