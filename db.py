import MySQLdb


class DBConnection(object):

    def __init__(self, db_host, db_user, db_pass, db_name, db_port=3306, path="/tmp/"):

        self.db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name, port=db_port)
        self.cur = self.db.cursor()
        self.path = path

    def execute(self, query):
        try:
            self.cur.execute(query)
        except MySQLdb.ProgrammingError:
            print "Error in running mysql query: %s" % query
        return self.cur.fetchall()

    def create_outfile(self, table_name, field_list, start, end, file_no):
        fields = ",".join(["lower(%s)" % field for field in field_list[1:]])
        fields = "%s,concat_ws(' ', %s)" % (field_list[0], fields)
        table_name = table_name
        query = "select %s from %s limit %s,%s into outfile '/%s/%s%s.txt'" % (fields, table_name, start, end,
                                                                               self.path,table_name, file_no)
        print query
        return self.execute(query)

    def get_table_counts(self, table_name):
        query = "select count(*) from %s" % table_name
        data_count = self.execute(query)
        if data_count:
            return data_count[0][0]
        return 0

    def get_all_records(self, field_list, table_name, where=""):
        fields = ",".join(["lower(%s)" % field for field in field_list[1:]])
        fields = "%s,concat_ws(' ', %s)" % (field_list[0], fields)
        query = "select %s from %s %s" % (fields, table_name, where)
        print query
        return self.execute(query)

    def close(self):
        self.db.close()