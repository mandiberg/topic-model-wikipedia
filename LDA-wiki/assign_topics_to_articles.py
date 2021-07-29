

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
    mycursor.execute("CREATE TABLE article_topics (id VARCHAR(20), enwiki_title VARCHAR(500), topic_no VARCHAR(20), score VARCHAR(500))")


    # Load model from disk.
    # change the model name here:
    model_lda = gensim.models.LdaModel.load("Model July 23/model_lda_July23")


    # change the processed doc name here:
    with open("processed_docs_500000_Jul-23-2021.txt", "r") as fp:
        b = json.load(fp)

        #load dict from disk.
        # change the dict name here:
        dictionary = gensim.corpora.Dictionary.load("dict_500000_Jul-23-2021")
        dictionary.filter_extremes(no_below=15, no_above=0.5, keep_n=100000)
        bow_corpus = [dictionary.doc2bow(doc) for doc in b]

    #assign topics to articles (through processed doc)
    for _ in range(len(b)):
        # print(b[_][-1])
        id = b[_][-1]
        # print(id)
        title = b[_][-2]
        title = title.replace("'","''")
        # print(title)
        first = True
        for index, score in sorted(model_lda[bow_corpus[_]], key=lambda tup: -1*tup[1]):
            if first:
                # print(index)
                # print(score)
                # print("-------------------------")
                # print("\nScore: {}\t \n {}\t \nTopic: {}".format(score, index, model_lda.print_topic(index, 10)))
                qr = f"INSERT INTO article_topics(id, enwiki_title, topic_no, score) VALUES('{id}', '{title}', '{index}', '{score}')"
                mycursor.execute(qr)
                mydb.commit()
                first = False
            else:
                break
                    # print(bow_corpus[])
