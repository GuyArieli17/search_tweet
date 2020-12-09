import io
import pickle
import zlib
import os
import concurrent.futures
from multiprocessing import Lock
from asyncio import Semaphore
import threading
import utils
import asyncio

class MapReduce:
    def __init__(self, MAX_LINE_IN_FILE = 5000,thread_pool_size = 2,path = 'MapReduceData/', meta_data = {},prev_byte = 0,file_index = 0):
        self.meta_data = meta_data #{term: [(self.file_index, self.line_number, number_of_lines)]]
        self.prev_byte = prev_byte
        self.file_index = file_index
        self.thread_pool_size = thread_pool_size
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=thread_pool_size)
        self.MAX_IN_FILE = MAX_LINE_IN_FILE
        self.update_lock = Lock()
        self.path = path
        self.meta_data_save = self.path + 'MetaData'
        self.number_of_current_thread = 0
        self.number_of_current_thread_lock = Lock()
        # self.number_of_current_thread_bool = True

    def wait_untill_finish(self):
        while self.number_of_current_thread != 0:
            pass

    def update_meta_data(self,term,number_of_bytes):
        #  if seen first time create a empty one
        if term not in self.meta_data.keys():
            self.meta_data[term] = []
        self.meta_data[term].append((self.file_index, self.prev_byte, number_of_bytes))
        self.prev_byte += number_of_bytes

    def update_files(self):
        if self.prev_byte >= self.MAX_IN_FILE:
            self.prev_byte = 0
            self.file_index += 1

    def write_dict(self,dic):
        self.number_of_current_thread_lock.acquire()
        self.number_of_current_thread += 1
        self.number_of_current_thread_lock.release()
        self.executor.submit(self.write_dict_func(dic))

    def write_dict_func(self,dic_org):
        dic = dic_org.copy()
        dic_org.clear()
        try:
            for key, value in dic.items():
                self.write_in(key, value)
        finally:
            self.number_of_current_thread_lock.acquire()
            self.number_of_current_thread -= 1
            self.number_of_current_thread_lock.release()
            dic.clear()
            return True

    def write_in(self,term, data_list):
        #  file name = (dic location) / the possible file to write in
        file_name = self.path + str(self.file_index)
        # only one process can update
        self.update_lock.acquire()
        # add data_list to file_name
        number_of_bytes = self.append_line([data_list], file_name)
        # save as (file_index,line_number_start,length)
        self.update_meta_data(term,number_of_bytes)
        # update number of line after add of data_list
        self.update_files()
        self.update_lock.release()

    def append_line(self, data_list, file_name):
        """
        Gets: file name and data to save
        Does: save the data as bytes and compress them
        """
        #add to the file
        number_of_bytes = 0
        with open(file_name + '.comp', "ab") as fd:
            if isinstance(data_list,list):
                for data_tuple in data_list:
                    # pickle.dump(data_tuple, fd, pickle.HIGHEST_PROTOCOL)
                    bytes = io.BytesIO()
                    # convert data into bytes as Bytes
                    pickle.dump(data_tuple, bytes)
                    # zbytes = zlib.compress(bytes.getbuffer())
                    zbytes = bytes.getbuffer()
                    number_of_bytes += len(zbytes)
                    fd.write(zbytes)


            # how to add dic
            elif isinstance(data_list,dict):
                bytes = io.BytesIO()
                # convert data into bytes as Bytes
                pickle.dump(data_list, bytes)
                # compress the byte and insert into zbytes
                zbytes = zlib.compress(bytes.getbuffer())
                number_of_bytes += len(zbytes)
                fd.write(zbytes)
        return number_of_bytes

    def read_from(self, term):
        """We want to return the list of doc with info about the term"""
        future = self.executor.submit(self.read_from_func,term)
        return future.result()

    def read_from_func(self, term):
        return self.read_from_func_async(term)

    def read_from_func_async(self,term): #async
        data_list = []
        # {(term: [])} #term=document,docID
        #await self.process_semaphore.acquire()
        try:
            term_meta_data = self.meta_data[term]
            # save dic so duplicate will be together
            dic_by_file_name = {} # {file_name: list of byte index in file}}
            # run and build file and all lines in this file
            for file_index, prev_byte,number_of_bytes in term_meta_data:
                if file_index not in dic_by_file_name.keys():
                    dic_by_file_name[file_index] = []
                dic_by_file_name[file_index].append((prev_byte,number_of_bytes))

            organize_dic = {} # {(file_index,line_number)}
            # read file by file
            for file_index,file_byte_lst in dic_by_file_name.items():
                # update file name with path
                file_name = self.path + str(file_index)
                # read
                dic_of_current_text_by_line = self.read_line(file_name, file_byte_lst)
                for line_index, line_object in dic_of_current_text_by_line.items():
                    organize_dic[(file_index, line_index)] = line_object
            # {term: [(self.file_index, self.line_number, number_of_lines)]]
            # build list as organize
            for i in range(len(term_meta_data)):
                current_file_index, current_line_number, _ = term_meta_data[i]
                if isinstance(organize_dic[(current_file_index, current_line_number)],dict):
                    data_list += (organize_dic[(current_file_index, current_line_number)],0)
                else:
                    data_list += organize_dic[(current_file_index, current_line_number)]
        finally:
            return data_list

    def read_line(self, file_name,file_byte_lst):
        """
        Gets: set of line to read from fuke
        Does: return the original object of file
        """
        # file byte lst = [(prev_byte,number_of_bytes)] [(5,7)]
        data = {} #{bytesNumber: obj}
        #sort by index
        file_byte_lst.sort(key=lambda x:x[0]) #[(fromWhere,numbersOfBytes)]
        data_str = {}
        if os.path.isfile(file_name + '.comp'):
            with open(file_name + '.comp', 'rb') as fd:
                prev_bytes = 0
                # text = pickle.load(fd)
                # run on all file lines
                start = -1
                for i, line in enumerate(fd):
                    index = 0
                    data_to_read_from_line = []  # list of data to read from line i
                    # line as string
                    array_line = bytes(line)
                    done_loop = False
                    # run and build the set of data in line
                    data_edit=0
                    while index < len(file_byte_lst) and not done_loop:
                        if file_byte_lst[index][0] >= prev_bytes and file_byte_lst[index][0]<prev_bytes+len(array_line):
                            done_loop = True
                            data_to_read_from_line.append(file_byte_lst[index])
                            data_edit+=1
                        index += 1
                    # remove to one we added to data_set
                    if done_loop:
                        file_byte_lst=file_byte_lst[:data_edit-1]
                    #line_bytes = 0
                    # data we need to keep search for
                    data_to_keep_readig = []
                    # run on line and build the set
                    for obj_index in range(len(data_to_read_from_line)): #[()]
                        # get the data inside the tuple
                        obj_start_from_start, obj_delta = data_to_read_from_line[obj_index]
                        # get where object start in line
                        obj_start_in_line = obj_start_from_start - prev_bytes
                        # get how much we need to read
                        left_to_read = (obj_delta - len(array_line))+obj_start_in_line #2
                        # first time for this object
                        if start==-1:
                            start=obj_start_from_start
                        if start not in data_str.keys():
                            data_str[start] = bytearray()  #{5:"b"}
                        # all the data in the line
                        if left_to_read <= 0:
                            data_str[start] += array_line[obj_start_in_line:obj_start_in_line+obj_delta]
                            start=-1
                            #line_bytes += obj_delta
                        # need to read more in next line
                        else:
                            # save all line
                            data_str[start] += array_line[obj_start_in_line:]
                            # change start to the new line
                            obj_start_from_start = prev_bytes+len(array_line)
                            # save what left to read (the new line , the delta  - how much we added)
                            data_to_keep_readig.append((obj_start_from_start,left_to_read))
                    if len(data_to_keep_readig)!=0:
                        file_byte_lst.insert(0,data_to_keep_readig[0])
                    prev_bytes += len(array_line)
                # de compress and convert to obj
                for data_str_key, data_str_value in data_str.items():
                    line_byte_code = bytes(data_str_value)
                    # byte_code = zlib.decompress(line_byte_code)
                    byte_code = line_byte_code
                    data[data_str_key] = pickle.loads(byte_code)
                    # data[data_str_key] = data_str_value
        return data

    def save_map_reduce(self):
        list = [self.MAX_IN_FILE, self.thread_pool_size, self.path, self.meta_data, self.prev_byte, self.file_index]
        utils.save_obj(list,self.meta_data_save)

    @staticmethod
    def import_map_reduce(path):
        file_name = path + 'MetaData'
        data = utils.load_obj(file_name)
        map_reduce = MapReduce(data[0], data[1], data[2], data[3], data[4], data[5])
        return map_reduce
