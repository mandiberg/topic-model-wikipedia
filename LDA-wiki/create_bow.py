if __name__ == '__main__':

    import json
    from gensim import corpora, models
    import gensim
    import pprint

    #load processed_docs json
    with open("processed_docs.txt", "r") as fp:
        b = json.load(fp)

        dictionary = gensim.corpora.Dictionary.load("dict")

        dictionary.filter_extremes(no_below=15, no_above=0.5, keep_n=100000)
        bow_corpus = [dictionary.doc2bow(doc) for doc in b]
        gensim.corpora.MmCorpus.serialize('BoW_corpus.mm', bow_corpus)

        bow_doc_4310 = bow_corpus[70]
        for i in range(len(bow_doc_4310)):
            print("Word {} (\"{}\") appears {} time.".format(bow_doc_4310[i][0], dictionary[bow_doc_4310[i][0]], bow_doc_4310[i][1]))


        tfidf = models.TfidfModel(bow_corpus)
        corpus_tfidf = tfidf[bow_corpus]
        for doc in corpus_tfidf:
            print(doc)
            break
        print("ok continue")
        lda_model = gensim.models.ldamulticore.LdaMulticore(bow_corpus, num_topics=20, id2word=dictionary, passes=2, workers=8)
        print("lda model implemented")
        for idx, topic in lda_model.print_topics(-1):
            print('Topic: {} \nWords: {}'.format(idx, topic))


     # lda_model_tfidf = gensim.models.LdaMulticore(corpus_tfidf, num_topics=20, id2word=dictionary, passes=2, workers=8)for idx, topic in lda_model_tfidf.print_topics(-1):print('Topic: {} Word: {}'.format(idx, topic))## processed_docs[4310]for index, score in sorted(lda_model[bow_corpus[70]], key=lambda tup: -1*tup[1]):print("nScore: {}t nTopic: {}".format(score, lda_model.print_topic(index, 20)))for index, score in sorted(lda_model_tfidf[bow_corpus[70]], key=lambda tup: -1*tup[1]):print("nScore: {}t nTopic: {}".format(score, lda_model_tfidf.print_topic(index, 20)))
        # unseen_document = 'How a Pentagon deal became an identity crisis for Google'
        # bow_vector = dictionary.doc2bow(preprocess(unseen_document))
        # print(unseen_document)
        # for index, score in sorted(lda_model[bow_vector], key=lambda tup: -1*tup[1]):
        #     print("Score: {}\t Topic: {}".format(score, lda_model.print_topic(index, 20)))
