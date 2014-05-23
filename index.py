from choices import DEFAULT_BUCKET_DICT as BUCKET_DICT
from stopwords import strip_stopwords
from db import DBConnection
from common import Commands
from Queue import Queue
import sys
import copy
import re
from multiprocessing import Pool


queue = Queue()

resplit = re.split

FILE_DICT = {}


def index_data(*args):
    start, offset, table_name, field_list, file_dict = args[0]
    index_table_name = "%s limit %s,%s" %(table_name, start, offset)
    temp_bucket = copy.deepcopy(BUCKET_DICT)
    bucket_dict = IndexData.run(field_list, index_table_name, temp_bucket)
    for k, v in BUCKET_DICT.items():
        file_dict[k] = open("/tmp/index_%s_%s.txt" % (table_name, k), "a+b")
    for k, v in bucket_dict.items():
        f = file_dict[k]
        for weight, pos in v.items():
            f.write("%s\t%s\n" % (weight, pos))
    for k in file_dict.keys():
        file_dict[k].close()


class IndexData(object):

    def __init__(self, file_name="local_config"):
        self.resultset = []
        settings = __import__("%s" % file_name)
        self.is_threading = settings.IMPLEMENT_THREADED_SEARCH
        db_config = settings.DATABASE
        self.index_classes = settings.INDEX_CLASSES
        self.is_indexing = settings.INDEXING
        self.intervals = settings.INTERVALS
        self.bucket_intervals = settings.BUCKET_INTERVAL
        self.word_split_pattern = settings.WORD_SPLIT_PATTERN
        self.conn = DBConnection(db_config['HOST'], db_config['USER'], db_config['PASSWORD'], db_config['NAME'],
                                 db_config['PORT'], settings.FILE_PATH)

    def split_sentence(self, raw_sentence):
        word_list = resplit(self.word_split_pattern, raw_sentence)
        return word_list

    def create_hash(self, sentence):
        word_list = self.split_sentence(sentence)
        weight_list = Commands.assign_weight(word_list)
        return weight_list

    def false_index(self, data_count, table_name, field_list):
        dump_file_counts = int(data_count/self.intervals) + 1
        start, offset = 0, self.intervals
        if not self.is_threading:
            offset = data_count
            dump_file_counts = 1
        for file_no in range(dump_file_counts):
            self.conn.create_outfile(table_name, field_list, start, offset, file_no)
            start += self.intervals

    def true_index(self, data_count, table_name, field_list):
        interval_count = int(data_count/self.intervals) if data_count > self.intervals else 1
        start, offset = 0, self.intervals
        file_dict = {}
        pool = Pool(processes=2)
        args_list = []
        for i in range(interval_count):
            args_list.append([start, offset, table_name, field_list, file_dict],)
            start += offset
        pool.map(index_data, args_list)
        pool.close()
        pool.join()

    def index(self):
        for instance in self.index_classes:
            data_count = self.conn.get_table_counts(instance.table_name)
            if data_count:
                getattr(self, ("%s_index" % self.is_indexing).lower())(data_count, instance.table_name,
                                                                       instance.field_list)
            else:
                print "No Data to Index. Exiting...."
        self.conn.close()

    @classmethod
    def run(cls, field_list, table_name, bucket={}):
        self = IndexData("local_config")
        result_set = self.conn.get_all_records(field_list, table_name)
        for pos, data in result_set:
                word_list = self.create_hash(data)
                for word, weight in word_list:
                    bucket_no = Commands.assign_bucket(weight, self.bucket_intervals)
                    try:
                        bucket[bucket_no][word].append(pos)
                    except KeyError:
                        bucket[bucket_no][word] = [pos, ]
        return bucket

if __name__ == "__main__":
    file_name = "local_config"
    if len(sys.argv) == 2:
        file_name = sys.argv[1]
    index_obj = IndexData(file_name)
    index_obj.index()

