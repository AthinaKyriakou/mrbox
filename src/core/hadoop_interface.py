import subprocess
import os
from core.mrbox_object import MRBoxObj
from utils.path_util import customize_path, remove_prefix
from utils.hdfs_util import hdfs_file_checksum, hdfs_file_size


class HadoopInterface:
    def __init__(self, hdfs_con, hadoop_path):
        """
        :param hdfs_con: from hdfs3
        :param hadoop_path: the local hadoop path
        """
        self.hdfsCon = hdfs_con
        self.hadoopPath = hadoop_path

    def rm(self, hdfs_path):
        self.hdfsCon.rm(hdfs_path)

    def put(self, local_path, hdfs_path):
        self.hdfsCon.put(local_path, hdfs_path)

    def walk(self, path):
        return self.hdfsCon.walk(path)

    def ls(self, path):
        return self.hdfsCon.ls(path)

    def exists(self, hdfs_path):
        return self.hdfsCon.exists(hdfs_path)

    def mkdir(self, hdfs_path):
        self.hdfsCon.mkdir(hdfs_path)

    def get(self, mrboxf):  # ok!
        """
        Get a file locally from hdfs, triggers on_created()
        :param mrboxf: the mrbox_file object
        :return:
        """
        if mrboxf.localType == 'link':
            with open(mrboxf.local_path, 'w+') as fp:
                fp.write(mrboxf.remote_path)  # todo: add correct link to be opened in browser
        else:
            self.hdfsCon.get(mrboxf.remote_path, mrboxf.local_path)

    def find_remote_paths(self, starting_path):
        """
        :param starting_path: the remote path of the file / dir that was deleted locally
        :param hdfs: connection to hdfs
        :return: list of remote paths of dirs + files in the file structure from starting_path (without /starting_path/)
        """
        print("find_remote_paths")
        list_of_paths = [starting_path]
        for sp, subdir, files in self.walk(starting_path):
            for name in subdir:
                list_of_paths.append(customize_path(sp, name))
            for name in files:
                list_of_paths.append(customize_path(sp, name))
        return list_of_paths

    def create_locally_synced_dir(self, cmd, lc, mrbox_dir):  # ok!
        """
        Creates a dir on hdfs by running the cmd command and creates a copy of it locally
        :param cmd: bash command to create hdfs dir
        :param lc: sqlite3 db class instance
        :param mrbox_dir: MRBox file object with the info regarding the dir that will be created locally + on HDFS
        :return:
        """

        subprocess.run(cmd, shell=True, check=True)
        hdfs_chk = hdfs_file_checksum(self.hadoopPath, mrbox_dir.remote_path, mrbox_dir.remote_file_type)
        lc.insert_tuple_hdfs(mrbox_dir.local_path, mrbox_dir.remote_path, hdfs_chk, mrbox_dir.localType)
        os.mkdir(mrbox_dir.local_path)  # creates an empty directory of hdfs outputs locally, triggers on_created()

        # decide if locally there will a link to the file on hdfs or the whole file
        # the same will be propagated to b2drop
        for rp in self.ls(mrbox_dir.remote_path):
            hdfs_chk = hdfs_file_checksum(self.hadoopPath, rp, 'file')
            f = remove_prefix(mrbox_dir.remote_path, rp)
            lp = customize_path(mrbox_dir.local_path, f)
            file_size = hdfs_file_size(self.hadoopPath, rp)
            mrbox_file = MRBoxObj(lp, mrbox_dir.local_file_size_limit, rp, file_size)
            lc.insert_tuple_hdfs(lp, rp, hdfs_chk, mrbox_file.localType)
            self.get(mrbox_file)
