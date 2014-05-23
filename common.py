import subprocess
import os
from choices import BUCKET_KEYS
from Queue import Queue
import re

queue = Queue()

CMD_OUTPUT = []


class SearchResult(object):

    def __init__(self, table_name):
        self.table_name = table_name

    def __repr__(self):
        return self.table_name


class Commands(object):

    def __init__(self, engine, operator="or", is_indexing=False):

        self.default_engine = engine
        self.operator = operator
        if is_indexing and self.operator == 'and':
            self.operator = 'or'

    def create_cmd(self, search_text_list, file_name):
        return getattr(self, "create_%s_cmd" % self.default_engine)(file_name, search_text_list)

    def create_awk_cmd(self, file_name, search_text_list):
        return getattr(self, "awk_%s_cmd" % self.operator)(file_name, search_text_list)

    def create_egrep_cmd(self, file_name, search_text_list):
        return getattr(self, "egrep_%s_cmd" % self.operator)(file_name, search_text_list)

    def create_sed_cmd(self, file_name, search_text_list):
        return getattr(self, "sed_%s_cmd" % self.operator)(file_name, search_text_list)

    @staticmethod
    def assign_weight(word_list):
        ascii_list = []
        lappend = ascii_list.append
        for word in word_list:
                ascii_sum = sum(bytearray(word))
                lappend((word, ascii_sum,))
        return ascii_list

    @staticmethod
    def assign_bucket(weight, bucket_intervals):
        bucket_no = weight/bucket_intervals
        if bucket_no > 10:
            bucket_no = 10
        return bucket_no

    @staticmethod
    def process_index_data(cmd_output):
        result = re.findall('\d+', "".join(cmd_output))
        return result

    @staticmethod
    def process_data(instance, cmd_output):
        result = []
        rows = cmd_output.split('\n')
        for row in rows:
            row = row.split('\t')
            row_len = len(row)
            if row_len > 1:
                result.append(row[0])
        return result

    @staticmethod
    def awk_or_cmd(file_name, search_text_list=[]):
        converted_text = "/" + '/ && /'.join(search_text_list) + "/"
        cmd_list = ["awk", "%s" % converted_text, file_name, ]
        return cmd_list

    @staticmethod
    def awk_and_cmd(file_name, search_text_list=[]):
        converted_text = "/" + '/ || /'.join(search_text_list) + "/"
        cmd_list = ["awk", "'%s'" % converted_text, file_name, ]
        return cmd_list

    @staticmethod
    def egrep_or_cmd(file_name, search_text_list=[]):
        converted_text = "|".join(search_text_list)
        cmd_list = ["egrep", converted_text, file_name, ]
        return cmd_list

    @staticmethod
    def egrep_and_cmd(file_name, search_text_list=[]):
        cmd_list = ["egrep", search_text_list.pop(), "%s" % file_name, ]
        for search_text in search_text_list:
            cmd_list.extend(['|', 'egrep', search_text])
        return cmd_list

    @staticmethod
    def sed_or_cmd(file_name, search_text_list=[]):
        converted_text = ";".join(['/%s/p' % term for term in search_text_list])
        cmd_list = ["sed", "-n", converted_text, file_name, ]
        return cmd_list

    @staticmethod
    def sed_and_cmd(file_name, search_text_list=[]):
        converted_text = "/%s/!d;".join(search_text_list)
        cmd_list = ["sed", converted_text, file_name, ]
        return cmd_list

    @staticmethod
    def remove_file(self, file_name):
        os.remove(file_name)


def exec_cmd(cmd_args):
    p = subprocess.Popen(" ".join(cmd_args), shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    output = p.communicate()[0]
    return output