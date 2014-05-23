import sys
import re
from Queue import Queue
from threading import Thread
from db import DBConnection
from common import Commands, exec_cmd
from choices import BUCKET_KEYS


queue = Queue()

CMD_OUTPUT = []


class SimpleSearch(object):

    def __init__(self, file_name="local_config"):
        self.resultset = []
        settings = __import__("%s" % file_name)
        self.is_threading = settings.IMPLEMENT_THREADED_SEARCH
        db_config = settings.DATABASE
        self.index_classes = settings.INDEX_CLASSES
        self.is_indexing = settings.INDEXING
        self.intervals = settings.INTERVALS
        self.file_path = settings.FILE_PATH
        self.engine = settings.ENGINE
        self.operator = settings.DEFAULT_OPERATOR
        self.text_based_search = None
        if self.is_indexing and hasattr(settings, "TEXT_BASED_SEARCH"):
            self.text_based_search = settings.TEXT_BASED_SEARCH
        if hasattr(settings, "BUCKET_INTERVAL"):
            self.bucket_intervals = settings.BUCKET_INTERVAL
        self.thread_timeout = 0.5
        if hasattr(settings, "THREAD_TIMEOUT"):
            self.thread_timeout = settings.THREAD_TIMEOUT
        self.ids_only = False
        if hasattr(settings, "IDS_ONLY") and settings.IDS_ONLY:
            self.ids_only = True
        self.conn = DBConnection(db_config['HOST'], db_config['USER'], db_config['PASSWORD'], db_config['NAME'])
        self.cmd_obj = Commands(self.engine, self.operator, self.is_indexing)

    def search(self, search_text):
        search_text_list = re.split("[ ,\-,_,',\",=,.,:,;]", search_text)
        result_list = []
        for instance in self.index_classes:
            if not self.is_indexing:
                data_count = int(self.conn.get_table_counts(instance.table_name)) + 1
                interval_count = int(data_count/self.intervals) if data_count > self.intervals else 1
                id_list = self.simple_search(instance, interval_count, search_text_list)
            else:
                weight_list = Commands.assign_weight(search_text_list)
                minimum_weight = min([weight for word, weight in weight_list])/self.bucket_intervals
                id_list = self.index_search(instance.table_name, search_text_list, minimum_weight, self.is_threading, self.thread_timeout)
            if id_list and not self.ids_only:
                where = "where id in (%s)" % ", ".join(id_list[:100000])
                result_list.extend(self.conn.get_all_records(instance.field_list, instance.table_name, where))
        return result_list or id_list

    def simple_search(self, instance, data_count, search_text_list):
        threads = []
        if not self.is_threading:
            data_count = 1
        for i in range(data_count):
            queue.put([i, instance, search_text_list])
            t = Thread(target=self.run)
            t.daemon = True
            t.start()
            threads.append(t)
        for t in threads:
            t.join(self.thread_timeout)
        return self.resultset

    def run(self):
        while not queue.empty():
            i, instance, search_text_list = queue.get()
            file_name = self.file_path + "%s%s.txt" % (instance.table_name, i)
            awk_cmd = self.cmd_obj.create_cmd(search_text_list, file_name)
            output = exec_cmd(awk_cmd)
            result = Commands.process_data(instance, output)
            self.resultset.extend(result)
            queue.task_done()


    def configure_buckets(self, word_list, bucket_list):
        temp_list = bucket_list
        if self.text_based_search:
            temp_list = []
            weight_list = self.cmd_obj.assign_weight(word_list)
            for word, weight in weight_list:
                temp_list.append(self.cmd_obj.assign_bucket(weight, self.bucket_intervals))
        return temp_list

    def file_search(self):
        global CMD_OUTPUT
        while not queue.empty():
            file_prefix, table_name, bucket_no, search_text_list, file_prefix = queue.get()
            file_name = "%sindex_%s_%s.txt" % (file_prefix, table_name, bucket_no)
            cmd_list = self.cmd_obj.create_cmd(search_text_list, file_name)
            result = exec_cmd(cmd_list)
            CMD_OUTPUT.append(result)
            queue.task_done()

    def index_search(self, table_name, search_text_list, minimum_weight, is_threading=False, thread_timeout=0.2,
                     file_prefix="/tmp/"):
        threads = []
        global CMD_OUTPUT
        search_text_id_dict = {}
        bucket_keys = self.configure_buckets(search_text_list, BUCKET_KEYS)
        for search_text in search_text_list:
            for bucket_no in bucket_keys:
                if bucket_no >= minimum_weight:
                    queue.put([file_prefix, table_name, bucket_no, [search_text, ], file_prefix, ])
                    t = Thread(target=self.file_search)
                    t.daemon = True
                    t.start()
                    threads.append(t)
                    if not is_threading:
                        t.join(thread_timeout)
            for t in threads:
                t.join(thread_timeout)
            id_list = Commands.process_index_data(CMD_OUTPUT)
            search_text_id_dict[search_text] = id_list
            CMD_OUTPUT = []
        lists = search_text_id_dict.values()
        try:
            intersected = set(lists[0]).intersection(*lists)
        except ValueError:
            intersected = set()
        return list(intersected)


if __name__ == "__main__":
    search_term = sys.argv[1]
    file_name = "local_config"
    if len(sys.argv) > 2:
        file_name = sys.argv[2]
    search_obj = SimpleSearch(file_name)
    print search_obj.search(search_term)


