

Dependencies

MySQL-Python

How It Works

you need to create a configuration file somewhere in the machine and have to pass it as argument while indexing or
searching. A test configuration file 'local_config.py' is already present in package

Add your mysql settings

DATABASE = {
    'NAME': 'metadataorder', # name of you DB
    'USER': 'root',
    'PASSWORD': '',
    'HOST': 'localhost',
}

Define classes

class ABC(object):
    field_list = ['id', 'name', ] # comma separated field names here. First element should always be an ID or primary key for the table
    table_name = "mysql table name here"

Add classes to INDEX_CLASSES

INDEX_CLASSES = [ABC, ]

Types Of Search

1.  Simple Search (records<=2,00,000)
    This will dump your entire table data at the location mentioned in FILE_PATH and will search for the keyword in entire dump

2.  Simple Threaded Search (records<=4,00,000)
    Add,
    IMPLEMENT_THREADED_SEARCH = True
    INTERVALS = 50000
    This will dump records in different files with each files having no. of records mentioned in INTERVALS variable.
    So, total no. files will be int(Total no. of records in table/INTERVAL) + 1 with each file having 50000 records.
    Now each thread will search for keywords in separately but at the same time. Individual results will be aggregated at the end.

3.  Indexed Based Searching (records<=20,00,000)
    Add,
    INDEXING = True
    In this mode each line is broken into words and a hash is generated for each word. Each word then goes into it's bucket as per calculated hash.
    During search hash is generated for each keyword. So search only occurs in one particular file based on hash instead of all the files.
    you can also add IMPLEMENT_THREADED_SEARCH = True to further speed up

WORD_SPLIT_PATTERN - Add your own sentence split patterns. Default pattern is "\W|_"

INTERVALS - It's the value which determines no. of records in each index file. Increase or decrease it's value to speed up indexing process

