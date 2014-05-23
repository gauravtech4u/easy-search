ENGINE = "sed"

DATABASE = {
    'NAME': 'updates',
    'USER': 'root',
    'PASSWORD': '',
    'HOST': 'localhost',
    'PORT': 3306,
}

FILE_PATH = '/tmp/'

INDEXING = True

TEXT_BASED_SEARCH = False


class TABLEABC(object):
    field_list = ['id', 'name', ]
    table_name = "my_mysql_table_name"


INDEX_CLASSES = [TABLEABC, ]

DEFAULT_OPERATOR = "and"

INTERVALS = 50000

IMPLEMENT_THREADED_SEARCH = True

BUCKET_INTERVAL = 100

WORD_SPLIT_PATTERN = "\W|_"

THREAD_TIMEOUT = 5

IDS_ONLY = True



