from parser_module import Parse
from ranker import Ranker
import utils

class Searcher:

    def __init__(self, inverted_index):
        #lalala
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker()
        self.inverted_index = inverted_index

    def relevant_docs_from_posting(self, query, posting):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        # { doc_id: [doc tuple, list of the same terms](list in list)}
        relevant_docs = {}
        meta_data_dict = dict()
        for index in range(len(query)):
            # for each document we will have the word they have the
            term = query[index]
            try:  # an example of checks that you have to do
                posting_doc = posting[term]  # list of all doc containt term
                meta_data_dict[index] = (term, len(posting_doc))  # {index:(term , number of doc with term)}
                for doc_tuple in posting_doc:
                    doc = doc_tuple[0]
                    if doc not in relevant_docs.keys():
                        relevant_docs[doc] = [doc_tuple[1], {index}] #doc_tuple[1]=freq
                    else:
                        relevant_docs[doc][1].add(index)
            except:
                print('term {} not found in posting'.format(term))
        relevant_docs["META-DATA"] = meta_data_dict
        return relevant_docs


        # for term in query:
        #     try: # an example of checks that you have to do
        #         posting_doc = posting[term] # list of all doc containt term
        #         for doc_tuple in posting_doc:
        #             doc = doc_tuple[0]
        #             if doc not in relevant_docs.keys():
        #                 relevant_docs[doc] = 1
        #             else:
        #                 relevant_docs[doc] += 1
        #     except:
        #         print('term {} not found in posting'.format(term))
        return relevant_docs

        # q : donald trump had corana last week
        # q-parse:  donald trump corana last week
        # q-terms: [donald, trump,donald trump , corona, last,week]
        # covid week impact donald trump  = > [covid,week, impact,donald,trump,donald trump]
        # doc has all word in englist lung
      #
        # tf =  number of term in doc/number of word in doc
        # idf = log( N(number of documents)/ number of doc with term)
        # w = tf * idf
        #
        #
        #
