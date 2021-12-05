

if __name__ == '__main__':

    import mysql.connector
    import gensim
    import json
    import databaseconfig as cfg

    from gensim.corpora.mmcorpus import MmCorpus


    mydb = mysql.connector.connect (
        host= cfg.mysql["host"],
        user=cfg.mysql["user"],
        password=cfg.mysql["password"],
        database=cfg.mysql["database"]
    )
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE article_topics150T_Sept6_secondtopic (id VARCHAR(20), enwiki_title VARCHAR(500), topic_no1 VARCHAR(20), score1 VARCHAR(500), topic_no2 VARCHAR(20), score2 VARCHAR(500), topic_no3 VARCHAR(20), score3 VARCHAR(500))")


    # Load model from disk.
    # change the model name here:
    model_lda = gensim.models.LdaModel.load("model_lda_full_Sep6_150T")


    # change the processed doc name here:
    with open("processed_docs_1000_Aug-18-2021.txt", "r") as fp:
        b = json.load(fp)

        #load dict from disk.
        # change the dict name here:
        dictionary = gensim.corpora.Dictionary.load("dict_full_Aug-28-2021")
        dictionary.filter_extremes(no_below=15, no_above=0.5, keep_n=100000)
        bow_corpus = [dictionary.doc2bow(doc) for doc in b]

    #assign topics to articles (through processed doc)
  
  #for testing, run a specific numberg
    #for _ in range(1000):
  #for production run all
    for _ in range(len(b)):
        # print(b[_][-1])
        id = b[_][-1]
        #print(id)
        title = b[_][-2]
        title = title.replace("'","''")
        # print(title)
        first = True
        counter = 0
        list_match1 = [0.1,0]
        list_match2 = [0.1,0]
        # print("-------------------------")
        # print(title)
        # print(id)
        # print("-------------------------")

        for index, score in sorted(model_lda[bow_corpus[_]], key=lambda tup: -1*tup[1]):
            print(counter)
            counter = counter+1

            if counter == 1:
                list_match1 = [index,score]
                # print("in counte1")
                # print(list_match1)


# if you want to match the article to a second topic you need to uncomment the code below
# this will generate first and second topics, but there will be less total results 
# because not all articles will produce a second topic. (about 3% based off limited testing)
            # elif counter == 2:
            #     list_match2 = [index,score]

# you don't need to uncomment these lines, unless you want terminal output of the data
            #     print("in counter 2")
            #     print(list_match2)
 


# need to change this db to match the name of db created above
                qr = f"INSERT INTO article_topics150T_Sept6_secondtopic(id, enwiki_title, topic_no1, score1, topic_no2, score2) VALUES('{id}', '{title}', '{list_match1[0]}', '{list_match1[1]}', '{list_match2[0]}', '{list_match2[1]}')"
                mycursor.execute(qr)
                mydb.commit()
                first = False
                break
