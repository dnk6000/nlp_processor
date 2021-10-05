


def LemmatizeAddrBase():
    #connect to PG
    GD = None
    import ModulesPyOnly.plpyemul as plpyemul
    plpy = plpyemul.get_plpy()    
    from Modules.Common.globvars import GlobVars
    gvars = GlobVars(GD)
    import Modules.Common.pginterface as pginterface
    cass_db = pginterface.MainDB(plpy, GD)
    #connect to PG

    import Modules.Parsing.lemma as lemma
    lemmatizer = lemma.Lemmatizer()

    socr_recs = cass_db.custom_simple_request('SELECT scname FROM addr.fias_socrbase WHERE NOT scname IS NULL;')
    socr = [i['scname'].strip() for i in socr_recs]

    counter = 0

    while True:
        counter += 1

        print(f'Batch: {counter} ####################################################################')

        non_lemmatized_recs = cass_db.custom_simple_request("SELECT \
                                                              id, \
                                                              full_addr, \
                                                              full_addr_lemm, \
                                                              full_addr_2, \
                                                              full_addr_2_lemm \
                                                            FROM  \
                                                              git010_dict.full_fias_addr \
                                                            WHERE \
	                                                            (full_addr != '' AND (full_addr_lemm IS NULL OR full_addr_lemm = '')) \
                                                                OR (full_addr_2 != '' AND (full_addr_2_lemm IS NULL OR full_addr_2_lemm = '')) \
                                                            LIMIT 1000")
        
        if len(non_lemmatized_recs) == 0:
            break
        
        _sentences = []
        _sentences_2 = []
        for i in non_lemmatized_recs:
            _sentences.append(i['full_addr'] if i['full_addr'] != '' else 'empty')
            _sentences_2.append(i['full_addr_2'] if i['full_addr_2'] != '' else 'empty')

        lemma_result = lemmatizer.lemmatize(_sentences)
        lemma_result_2 = lemmatizer.lemmatize(_sentences_2)

        for result in zip(non_lemmatized_recs, lemma_result, _sentences, lemma_result_2, _sentences_2):
            lemm_list = []
            for i in result[1]:
                if i['lemma'] == 'empty':
                    pass
                elif i['word'] in socr:
                    lemm_list.append(i['word'])
                else:
                    lemm_list.append(i['lemma'])
            lemm = ' '.join(lemm_list)

            lemm_list = []
            for i in result[3]:
                if i['lemma'] == 'empty':
                    pass
                elif i['word'] in socr:
                    lemm_list.append(i['word'])
                else:
                    lemm_list.append(i['lemma'])
            lemm_2 = ' '.join(lemm_list)


            print(f"id: {result[0]['id']} sent: {result[2]}   lemma: {lemm}   sent: {result[4]}  lemma_2:{lemm_2}")
            cass_db.custom_simple_request(f"UPDATE \
                                              git010_dict.full_fias_addr \
                                            SET \
                                              full_addr_lemm = '{lemm}', \
                                              full_addr_2_lemm = '{lemm_2}' \
                                            WHERE \
                                              id = {result[0]['id']}\
                                            ;")


LemmatizeAddrBase()