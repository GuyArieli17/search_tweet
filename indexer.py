from MapReduce import MapReduce
from parser_module import Parse
import psutil
import sys
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import concurrent.futures


class Indexer:

    def __init__(self, config, all_terms_dict):
        self.inverted_idx = all_terms_dict
        #self.postingDict = {}
        self.fileName = 'InvertedIndex'
        self.config = config
        # {term: [ordered list where appear : (file_id , lineNumber)]}
        self.thread_pool_size = 2
        avg_ram = (psutil.virtual_memory().available // self.thread_pool_size)//10
        path = 'MapReduceData/'
        self.avg_length =(avg_ram // sys.getsizeof((int(), str()))) // (8/10)
        # self.map_reduce = MapReduce(self.avg_length,self.thread_pool_size)
        self.map_reduce_ag = MapReduce(self.avg_length, self.thread_pool_size, path + 'AG/')
        self.map_reduce_hq = MapReduce(self.avg_length, self.thread_pool_size, path + 'HQ/')
        self.map_reduce_rz = MapReduce(self.avg_length, self.thread_pool_size, path + 'Rz/')
        self.map_reduce_other = MapReduce(self.avg_length, self.thread_pool_size, path + 'Others/')
        self.map_reduce_doc = MapReduce(self.avg_length, self.thread_pool_size, path + 'Document/')
        self.tmp_pos = {}
        # self.num_in_pos_tmp = 0
        self.num_in_pos_ag_tmp = [0]
        self.num_in_pos_hq_tmp = [0]
        self.num_in_pos_rz_tmp = [0]
        self.num_in_pos_other_tmp = [0]
        self.num_in_pos_doc_other = [0]
        self.Entitys = {}
        self.tmp_pos_ag = {}
        self.tmp_pos_hq = {}
        self.tmp_pos_rz = {}
        self.tmp_pos_other = {}
        self.tmp_pos_doc = {}
        # self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        self.NUMBER_OF_PROCESSES = 5

    def get_right_tmp_pos_and_num(self, first_letter):
        lower_letter = str(first_letter).lower()
        if 'a' <= lower_letter <= 'g':
            return [self.tmp_pos_ag, self.num_in_pos_ag_tmp, self.map_reduce_ag]
        elif 'h' <= lower_letter <= 'q':
            return [self.tmp_pos_hq, self.num_in_pos_hq_tmp, self.map_reduce_hq]
        elif 'r' <= lower_letter <= 'z':
            return [self.tmp_pos_rz, self.num_in_pos_rz_tmp, self.map_reduce_rz]
        return [self.tmp_pos_other, self.num_in_pos_other_tmp, self.map_reduce_other]


    def wait_untill_all_finish(self):
        self.map_reduce_ag.wait_untill_finish()
        self.map_reduce_hq.wait_untill_finish()
        self.map_reduce_rz.wait_untill_finish()
        self.map_reduce_other.wait_untill_finish()
        self.map_reduce_doc.wait_untill_finish()

    def save_left_over(self, dict,map_reduce):
        map_reduce.write_dict_func(dict)

    def check_save_left_over_ag(self):
        if self.num_in_pos_ag_tmp[0] > 0:
            self.save_left_over(self.tmp_pos_ag, self.map_reduce_ag)
            self.num_in_pos_ag_tmp[0] = 0

    def check_save_left_over_hq(self):
        if self.num_in_pos_hq_tmp[0] > 0:
            self.save_left_over(self.tmp_pos_hq, self.map_reduce_hq)
            self.num_in_pos_hq_tmp[0] = 0

    def check_save_left_over_rz(self):
        if self.num_in_pos_rz_tmp[0] > 0:
            self.save_left_over(self.tmp_pos_rz, self.map_reduce_rz)
            self.num_in_pos_rz_tmp[0] = 0

    def check_save_left_over_others(self):
        if self.num_in_pos_other_tmp[0] > 0:
            self.save_left_over(self.tmp_pos_other,self.map_reduce_other)
            self.num_in_pos_other_tmp[0] = 0

    def check_save_left_over_doc(self):
        if self.num_in_pos_doc_other[0] > 0:
            self.save_left_over(self.tmp_pos_doc, self.map_reduce_doc)
            self.num_in_pos_doc_other[0] = 0

    def save_all_left_overs(self):
        # self.check_save_left_over_ag()
        # self.check_save_left_over_doc()
        # self.check_save_left_over_hq()
        # self.check_save_left_over_rz()
        # self.check_save_left_over_others()
        with ProcessPoolExecutor() as process_exector:
            process_exector.submit(self.check_save_left_over_ag())
            process_exector.submit(self.check_save_left_over_hq())
            process_exector.submit(self.check_save_left_over_rz())
            process_exector.submit(self.check_save_left_over_others())
            process_exector.submit(self.check_save_left_over_doc())

    def add_entitys_to_posting(self, term, tweet_id, quantity):
        first_letter = term[0]
        tmp_pos, number_arr, _ = self.get_right_tmp_pos_and_num(first_letter)
        if term.upper() not in self.Entitys.keys() and term.upper() not in tmp_pos.keys():
            self.Entitys[term.upper()] = (tweet_id, quantity)
        else:
            if term.upper() not in self.inverted_idx.keys():
                self.inverted_idx[term.upper()] = 2
            else:
                self.inverted_idx[term.upper()] += 1
            if term.upper() not in tmp_pos.keys():
                tmp_pos[term.upper()] = []
                tmp_pos[term.upper()].append(self.Entitys[term.upper()])
                del self.Entitys[term.upper()]
            tmp_pos[term.upper()].append((tweet_id, quantity))

    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """
        document_dictionary = document.term_doc_dictionary #{term:freq,term:freq}
        term_lst = [*document_dictionary]
        term_lst.sort(key=lambda x: x.lower())
        for i in range(len(term_lst)):
            term = term_lst[i]
            tmp_pos, number_arr, map_reduce = self.get_right_tmp_pos_and_num(term[0])
            try:
                if term[0].isupper() and " " in term:
                    self.add_entitys_to_posting(term,document.tweet_id, document_dictionary[term])
                    continue
                if number_arr[0] >= self.avg_length:
                    map_reduce.write_dict_func(tmp_pos)
                    number_arr[0] = 0
                if term.lower() not in self.tmp_pos.keys():
                    tmp_pos[term.lower()] = []
                tmp_pos[term.lower()].append((document.tweet_id, document_dictionary[term]))
                number_arr[0] += 1
            except:
                print('problem with the following key {}'.format(term[0]))
        max_freq = max([document_dictionary.values()])
        self.tmp_pos_doc[document.tweet_id] = document_dictionary
        self.num_in_pos_doc_other[0] += 1
        if self.num_in_pos_doc_other[0] >= self.avg_length:
            self.map_reduce_doc.write_dict_func(self.tmp_pos_doc)
            self.num_in_pos_doc_other[0] = 0


if __name__ == '__main__':
    p = Parse(True)
    parsed_document = p.parse_doc(['1280914835979501568', 'Wed Jul 08 17:21:09 +0000 2020', '70% @loganxtalor: Y’all Towson took away my housing cause of COVID and I literally didn’t know where I was gonna go. I was in such a bind. I…', '{}', '[]',
                                   'Y’all Towson took away my housing cause of COVID and I literally didn’t know where I was gonna go. I was in such a… https://t.co/i8IdrIKp2B', '{"https://t.co/i8IdrIKp2B":"https://twitter.com/i/web/status/1280659984628490246"}', '[[116,139]]', None, None, None, None, None, None])
    i = Indexer()
    i.add_new_doc(parsed_document)

