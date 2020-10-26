import sqlite3
from sqlite3 import Error
import os

SQLITE_TRUE = 1
SQLITE_FALSE = 0

# todo: handle query failures


# prints part of an sql command based on the keys/values of the given dictionary
# Sample output: '(alpha, beta) VALUES ("gamma", "delta")'
def insert_substr(dic):
    items = dic.items()
    cols = [x[0] for x in items]
    vals = [x[1] for x in items]
    return '(' + ', '.join(cols) + ') VALUES (' + ', '.join(vals) + ')'


def update_substr(dic):
    return ', '.join([str(x) + '=' + str(dic[x]) for x in dic])


def asstr(s, qmark='"'):
    val = qmark + str(s) + qmark
    return val


class LocalCatalogue:
    TABLE_NAME = 'mrbox_files'

    # Column names: #####################################

    ID = 'id'
    LOC = 'local'               # local path of file/folder
    REM = 'remote'              # remote path of file/folder
    TIME_LOC = 'time_local'     # time of latest modification locally
    TIME_REM = 'time_remote'    # time of latest modification remotely
    CHK = 'checksum'            # to check validity of file, in binary

    #################################################################

    def __init__(self, full_db_path):
        self.db_path = full_db_path
        print('DB created successfully')
        if not self.check_table_exists():
            self.create_table()
        else:
            print('Table already exists')

    def create_connection(self):
        """ creates a db connection to the SQLite db of the local catalogue
        :return: Connection object or raises exception"""
        try:
            conn = sqlite3.connect(self.db_path)
            return conn
        except Error as e:
            print(e)
            raise

    def check_table_exists(self):
        cmd = "SELECT name FROM sqlite_master WHERE type='table' AND name=%s" % asstr(self.TABLE_NAME)
        conn = self.create_connection()
        c = conn.cursor()
        ret = len(c.execute(cmd).fetchall()) > 0
        conn.close()
        return ret

    def create_table(self):
        cmd = 'CREATE TABLE IF NOT EXISTS %s (%s INTEGER PRIMARY KEY,' % (self.TABLE_NAME, self.ID)
        cmd += '%s TEXT UNIQUE,' % self.LOC
        cmd += '%s TEXT UNIQUE,' % self.REM
        cmd += '%s DATETIME,' % self.TIME_LOC
        cmd += '%s DATETIME,' % self.TIME_REM
        cmd += '%s TEXT)' % self.CHK
        conn = self.create_connection()
        c = conn.cursor()
        c.execute(cmd)
        print('Table created successfully')
        conn.close()

    # todo: implement checksum, insync, linked, time_rem
    def insert_tuple(self, loc, remote, in_sync=SQLITE_TRUE, linked=SQLITE_FALSE):
        conn = self.create_connection()
        c = conn.cursor()
        dic = {self.LOC: asstr(loc),
               self.REM: asstr(remote),
               self.TIME_LOC: asstr('datetime("now")', qmark=''),
               self.TIME_REM: asstr(None),
               self.CHK: asstr(None)}
        cmd = "INSERT OR IGNORE INTO %s %s" % (self.TABLE_NAME, insert_substr(dic))
        c.execute(cmd)
        conn.commit()
        conn.close()

    def get_remote_file_path(self, local_path):  # todo: check how the exception will be handled
        """ Queries the SQLite db to get the remote_path corresponding to the given local_path
        :param local_path: path of file/folder locally
        :return: path of file/folder remotely
                    or raises exception if local_path does not exist in the db and returns None"""

        conn = self.create_connection()
        c = conn.cursor()
        cmd = "SELECT remote FROM %s WHERE local=%s" % (self.TABLE_NAME, asstr(local_path))
        c.execute(cmd)
        ret = c.fetchall()  # returns a list of tuples
        conn.close()
        try:
            remote_path = ret[0][0]
            return remote_path
        except IndexError:
            print("Local path " + local_path + " does not correspond to a remote path")

    def set_modified_local(self, local_path, in_sync=SQLITE_TRUE):  # todo: impl checksum, in_sync, linked, time_rem
        self.set_modified(local_path, self.LOC, in_sync)

    def set_modified(self, local_path, column_name, in_sync):
        conn = self.create_connection()
        c = conn.cursor()
        d = {self.CHK: asstr(None),
             self.TIME_LOC: asstr('datetime("now")', qmark='')}
        cmd = 'UPDATE %s SET %s WHERE %s="%s"' % (self.TABLE_NAME, update_substr(d), column_name, local_path)
        c.execute(cmd)
        conn.commit()
        conn.close()

    # todo: how will I handle the already deleted file str locally if transaction fails?
    #  inconsistency because file str does not exist locally, exist in hdfs + db
    def delete_by_local_path(self, list_of_local_paths):
        """ Deletes by given local paths in a transaction """
        conn = self.create_connection()
        conn.isolation_level = None
        c = conn.cursor()
        c.execute("begin")
        try:
            for lp in list_of_local_paths:
                cmd = 'DELETE FROM %s WHERE %s="%s"' % (self.TABLE_NAME, self.LOC, lp)
                c.execute(cmd)
            # c.execute("fnord")          # to check if transaction rollbacks
            conn.commit()
        except sqlite3.Error:
            print("Transaction failed!")
            conn.rollback()
        conn.close()

    # todo: how will I handle the already deleted file str locally if transaction fails?
    #  inconsistency because file str does not exist locally, exist in hdfs + db
    def delete_by_remote_path(self, list_of_remote_paths):
        """ Deletes by given remote paths in a transaction """
        conn = self.create_connection()
        conn.isolation_level = None
        c = conn.cursor()
        c.execute("begin")
        try:
            for rp in list_of_remote_paths:
                # srp = os.path.join(remote_starting_path, rp)
                # cmd = 'DELETE FROM %s WHERE %s="%s"' % (self.TABLE_NAME, self.REM, srp)
                cmd = 'DELETE FROM %s WHERE %s="%s"' % (self.TABLE_NAME, self.REM, rp)
                c.execute(cmd)
            # c.execute("fnord")          # to check if transaction rollbacks
            conn.commit()
        except sqlite3.Error:
            print("Transaction failed!")
            conn.rollback()
        conn.close()

    def update_by_remote_path(self, tuples_list):       # todo: can also happen by local if needed
        """
        Updates local and remote paths after a file / folder has moved, queries db table by local path
        :param tuples_list: list of tuples of (cur_remote_path, new_local_path, new_remote_path)
        :return:
        """
        conn = self.create_connection()
        conn.isolation_level = None
        c = conn.cursor()
        c.execute("begin")
        try:
            for t in tuples_list:
                cur_rp = t[0]
                new_lp = t[1]
                new_rp = t[2]
                cmd = 'UPDATE %s SET %s="%s", %s="%s" WHERE %s="%s"' % (self.TABLE_NAME, self.LOC, new_lp,
                                                                        self.REM, new_rp, self.REM, cur_rp)
                print(cmd)
                c.execute(cmd)
            conn.commit()
        except sqlite3.Error:
            print("Transaction failed!")
            conn.rollback()
        conn.close()
