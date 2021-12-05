if __name__ == '__main__':

    import json
    from gensim import corpora, models
    import gensim
    import pprint
    from gensim.test.utils import datapath


    #load processed_docs json
    #this is set to load the test doc with 1000 entries
    #for production switch to processed_docs_full_Aug-28-2021.txt
    with open("processed_docs_1000_Aug-18-2021.txt", "r") as fp:
        b = json.load(fp)

        dictionary = gensim.corpora.Dictionary.load("dict_full_Aug-28-2021")

        dictionary.filter_extremes(no_below=20, no_above=0.5, keep_n=100000)
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
        lda_model = gensim.models.ldamulticore.LdaMulticore(bow_corpus, num_topics=150, id2word=dictionary, passes=2, workers=8)
        print("lda model implemented")
        for idx, topic in lda_model.print_topics(-1):
            print('Topic: {} \nWords: {}'.format(idx, topic))

        # Save model to disk.
        temp_file = datapath("model")
        lda_model.save("model_lda_full_Sep6_150T")

