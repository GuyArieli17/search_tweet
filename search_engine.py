import time
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
from xlwt import Workbook
from MapReduce import MapReduce





def run_engine(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    """
    :return:
    """
    number_of_documents = 0

    config = ConfigClass(corpus_path, output_path, stemming)
    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse(stemming)
    indexer = Indexer(config, p.terms_dic_to_document)
    # Iterate over every document in the file
    for i in r.filesPath:
        documents_list = r.read_file(i)
        start_time = time.time()
        for idx, document in enumerate(documents_list):
            # parse the document
            parsed_document = p.parse_doc(document)
            # update the number of doc in system
            number_of_documents += 1
            # index the document data
            indexer.add_new_doc(parsed_document)
        # print(time.time() - start_time)
    print('--------------------------')
    print('Start writing to disk left overs')
    indexer.save_all_left_overs()
    print('Finish without waiting ' + str(time.time() - start_time))
    print('Start waiting')
    indexer.wait_untill_all_finish()
    print('End Waiting')
    print('Finished writing to disk left overs')
    print('--------------------------')
    print('Finished parsing and indexing. Starting to export files')
    print('Finish all Time ' + str(time.time() - start_time))
    utils.save_obj(indexer.inverted_idx, "inverted_idx")
    # indexer.map_reduce.save_map_reduce()

def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")
    return inverted_index

def search_and_rank_query(query, inverted_index,num_docs_to_retrieve):
    p = Parse()
    dictFromQuery = {}
    p.tokenSplit(query, dictFromQuery)
    query_as_list = [*dictFromQuery]
    searcher = Searcher(inverted_index)
    #posting = utils.load_obj("posting")
    print('-------------------------------------')
    print('Start import mapReduce')
    map_reduce = MapReduce.import_map_reduce('MapReduceData/')
    print('Done importing mapReduce')
    posting = {}
    print('-------------------------------------')
    print('Start build posting file')
    for term in query_as_list:
        posting[term] = map_reduce.read_from(term)
    print('Done building posting file')
    print('-------------------------------------')
    print('Get relevant Doc')
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list,posting)
    print('Done getting relevant Doc')
    print('-------------------------------------')
    print('Start ranking docs')
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs,dictFromQuery,posting,num_docs_to_retrieve)
    print('Done ranking docs')
    return searcher.ranker.retrieve_top_k(ranked_docs,num_docs_to_retrieve)

def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    wb = Workbook()
    counter = 1
    counterQuery = 0
    # add_sheet is used to create sheet.
    sheet1 = wb.add_sheet('Sheet 1')
    sheet1.write(0, 1, 'Query_number')
    sheet1.write(0, 2, 'Tweet_id')
    sheet1.write(0, 3, 'Rank')
    run_engine(corpus_path, output_path, stemming,queries, num_docs_to_retrieve)
    # query = input("Please enter a query: ")
    # k = int(input("Please enter number of docs to retrieve: "))
    inverted_index = load_index()
    for query in queries:
        start_time = time.time()
        d=search_and_rank_query(query, inverted_index, num_docs_to_retrieve)
        print(d)
        for doc_tuple in d:
            if doc_tuple[0]!='META-DATA':
                sheet1.write(counter,0,counter)
                sheet1.write(counter,1,counterQuery)
                sheet1.write(counter,2,doc_tuple[0])
                sheet1.write(counter,3,doc_tuple[1][0])
                counter+=1
        counterQuery+=1
        print('Time:' + str(time.time() - start_time))
    wb.save(output_path + '\\results.xls')

# print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
