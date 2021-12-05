

if __name__ == '__main__':

    import gensim
    import json
    from gensim.corpora.mmcorpus import MmCorpus


  

    # Load model from disk.
    # change the model name here:
    model_lda = gensim.models.LdaModel.load("model_lda_200T_July28/model_lda_200T_July28")
   # print (model_lda.show_topics(num_topics=10, num_words=10, log=False, formatted=True))

    for idx, topic in model_lda.print_topics(-1,50):
        print('Topic: {} \nWords: {}'.format(idx, topic))




 
