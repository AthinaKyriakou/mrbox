import sqlite3
from sqlite3 import Error
from utils.db_util import insert_substr, update_substr, asstr

# todo: handle query failures
# todo: might need to specify which HDFS in case multiple


class LocalCatalogue:
    TABLE_NAME = 'mrbox_files'

    # Column names: #####################################

    ID = 'id'
    LOC = 'local'                   # local path of file/folder
    HDFS = 'hdfs'                   # hdfs path of file/folder
    TIME_LOC = 'time_local'         # time of latest modification locally
    TIME_HDFS = 'time_hdfs'         # time of latest modification on hdfs
    CHK_LOC = 'chk_local'           # CRC32C checksum of local file copy
    CHK_HDFS = 'chk_hdfs'           # CRC32C checksum of HDFS file copy
    TYPE_LOC = 'type_loc'           # the type of the local file copy: 'file', 'link', 'dir'

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
            raise e

    def check_table_exists(self):  # todo: check error
        cmd = "SELECT name FROM sqlite_master WHERE type='table' AND name=%s" % asstr(self.TABLE_NAME)
        conn = self.create_connection()
        c = conn.cursor()
        ret = len(c.execute(cmd).fetchall()) > 0
        conn.close()
        return ret

    def create_table(self):  # todo: check error
        cmd = 'CREATE TABLE IF NOT EXISTS %s (%s INTEGER PRIMARY KEY,' % (self.TABLE_NAME, self.ID)
        cmd += '%s TEXT UNIQUE,' % self.LOC
        cmd += '%s TEXT UNIQUE,' % self.HDFS
        cmd += '%s DATETIME,' % self.TIME_LOC
        cmd += '%s DATETIME,' % self.TIME_HDFS
        cmd += '%s TEXT,' % self.CHK_LOC
        cmd += '%s TEXT' % self.CHK_HDFS
        cmd += '%s TEXT)' % self.TYPE_LOC
        conn = self.create_connection()
        c = conn.cursor()
        c.execute(cmd)
        print('Table created successfully')
        conn.close()

    def check_local_path_exists(self, local_path):
        return self.check_record_exists(self.LOC, local_path)

    def check_record_exists(self, col, col_val):
        conn = None
        try:
            conn = self.create_connection()
            c = conn.cursor()
            cmd = 'SELECT COUNT(1) FROM %s WHERE %s="%s"' % (self.TABLE_NAME, col, col_val)
            c.execute(cmd)
            ret = c.fetchall()  # returns a list
            conn.commit()
            if ret[0][0] > 0:
                return True
            else:
                return False
        except sqlite3.Error as e:
            raise e
        finally:
            if conn:
                conn.close()

    def insert_tuple_local(self, loc, rem, loc_chk):
        """
        When a file is created locally
        :param loc: local path
        :param rem: remote path
        :param loc_chk: checksum of file copy locally
        :return:
        """
        dic = {self.LOC: asstr(loc),
               self.HDFS: asstr(rem),
               self.TIME_LOC: asstr('datetime("now")', qmark=''),
               self.TIME_HDFS: asstr(None),
               self.CHK_LOC: asstr(loc_chk),
               self.CHK_HDFS: asstr(None)}
        self.insert_tuple(dic)

    def insert_tuple_hdfs(self, loc, rem, hdfs_chk):
        """
        When a file is created on hdfs and will be copied locally
        :param loc: local path
        :param rem: remote path
        :param hdfs_chk: checksum of file copy on hdfs
        :return:
        """
        dic = {self.LOC: asstr(loc),
               self.HDFS: asstr(rem),
               self.TIME_LOC: asstr(None),
               self.TIME_HDFS: asstr('datetime("now")', qmark=''),
               self.CHK_LOC: asstr(None),
               self.CHK_HDFS: asstr(hdfs_chk)}
        self.insert_tuple(dic)

    def insert_tuple(self, dic):
        conn = None
        try:
            conn = self.create_connection()
            c = conn.cursor()
            cmd = "INSERT OR IGNORE INTO %s %s" % (self.TABLE_NAME, insert_substr(dic))
            c.execute(cmd)
            conn.commit()
        except sqlite3.Error as e:
            raise e
        finally:
            if conn:
                conn.close()

    def update_tuple_local(self, local_path, loc_chk):
        dic = {self.TIME_LOC: asstr('datetime("now")', qmark=''),
               self.CHK_LOC: asstr(loc_chk)}
        self.update_tuple(self.LOC, local_path, dic)

    def update_tuple_hdfs(self, local_path, hdfs_chk):
        dic = {self.TIME_HDFS: asstr('datetime("now")', qmark=''),
               self.CHK_HDFS: asstr(hdfs_chk)}
        self.update_tuple(self.LOC, local_path, dic)            # update by local path, todo: create index

    def update_tuple(self, col, col_val, dic):
        """
        Updates an existing db tuple
        :param col: the db column by which the update is made
        :param col_val: the value of the db column
        :param dic: columns to update + their new values
        :return:
        """
        conn = None
        try:
            conn = self.create_connection()
            c = conn.cursor()
            cmd = 'UPDATE %s SET %s WHERE %s="%s"' % (self.TABLE_NAME, update_substr(dic), col, col_val)
            c.execute(cmd)
            conn.commit()
        except sqlite3.Error as e:
            raise e
        finally:
            if conn:
                conn.close()

    def get_remote_file_path(self, local_path):
        return self.get_val_by_local_path(self.HDFS, local_path)

    def get_loc_chk(self, local_path):
        return self.get_val_by_local_path(self.CHK_LOC, local_path)

    def get_hdfs_chk(self, local_path):
        return self.get_val_by_local_path(self.CHK_HDFS, local_path)

    def get_time_loc(self, local_path):
        return self.get_val_by_local_path(self.TIME_LOC, local_path)

    def get_time_hdfs(self, local_path):
        return self.get_val_by_local_path(self.TIME_HDFS, local_path)

    def get_val_by_local_path(self, col, local_path):  # todo: check error handling
        conn = self.create_connection()
        c = conn.cursor()
        cmd = "SELECT %s FROM %s WHERE %s=%s" % (col, self.TABLE_NAME, self.LOC, asstr(local_path))
        c.execute(cmd)
        ret = c.fetchall()  # returns a list of tuples
        conn.close()
        try:
            result = ret[0][0]
            return result
        except IndexError:
            print("Local path " + local_path + " does not exist in the db")
            return None

    # todo: how will I handle the already deleted file str locally if transaction fails?
    #  inconsistency because file str does not exist locally, exist in hdfs + db
    def delete_by_local_path(self, list_of_local_paths):   # todo: check error handling
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
    def delete_by_remote_path(self, list_of_remote_paths):   # todo: check error handling
        """ Deletes by given remote paths in a transaction """
        conn = self.create_connection()
        conn.isolation_level = None
        c = conn.cursor()
        c.execute("begin")
        try:
            for rp in list_of_remote_paths:
                # srp = os.path.join(remote_starting_path, rp)
                # cmd = 'DELETE FROM %s WHERE %s="%s"' % (self.TABLE_NAME, self.REM, srp)
                cmd = 'DELETE FROM %s WHERE %s="%s"' % (self.TABLE_NAME, self.HDFS, rp)
                c.execute(cmd)
            # c.execute("fnord")          # to check if transaction rollbacks
            conn.commit()
        except sqlite3.Error:
            print("Transaction failed!")
            conn.rollback()
        conn.close()

    # todo: check error handling
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
                                                                        self.HDFS, new_rp, self.HDFS, cur_rp)
                print(cmd)
                c.execute(cmd)
            conn.commit()
        except sqlite3.Error:
            print("Transaction failed!")
            conn.rollback()
        conn.close()
