import subprocess
import os
from utils.path_util import customize_path, remove_prefix
from utils.db_util import asstr


def find_remote_paths(starting_path, hdfs):
    """
    :param starting_path: the remote path of the file / dir that was deleted locally
    :param hdfs: connection to hdfs
    :return: list of remote paths of dirs + files in the file structure from starting_path (without /starting_path/)
    """
    print("find_remote_paths")
    list_of_paths = [starting_path]
    for sp, subdir, files in hdfs.walk(starting_path):
        for name in subdir:
            list_of_paths.append(customize_path(sp, name))
        for name in files:
            list_of_paths.append(customize_path(sp, name))
    return list_of_paths


def create_locally_synced_hdfs_dir(cmd, hdfs, lc, loc_path, hdfs_path, hadoop_path):
    """
    Creates a dir on hdfs by running the cmd command and creates a copy of it locally
    :param cmd: bash command to create hdfs dir
    :param hdfs: the connection to HDFS namenode
    :param lc: sqlite3 db class instance
    :param loc_path: the path that dir will be created locally
    :param hdfs_path: the path that dir will be created on hdfs
    :param hadoop_path: local path where hadoop is installed
    :return:
    """

    # todo: implement dir checksum
    subprocess.run(cmd, shell=True, check=True)
    hdfs_chk = hdfs_file_checksum(hadoop_path, hdfs_path, 'dir')
    lc.insert_tuple_hdfs(loc_path, hdfs_path, hdfs_chk)
    os.mkdir(loc_path)  # triggers on_created()

    for rp in hdfs.ls(hdfs_path):
        hdfs_chk = hdfs_file_checksum(hadoop_path, rp, 'file')
        f = remove_prefix(hdfs_path, rp)
        lp = customize_path(loc_path, f)
        lc.insert_tuple_hdfs(lp, rp, hdfs_chk)
        hdfs.get(rp, lp)  # triggers on_created()


def hdfs_file_checksum(hadoop_path, hdfs_filepath, ftype):
    if ftype == 'dir':
        return None
    cmd_hdfs_chk = customize_path(hadoop_path, 'bin/hdfs') + \
                   " dfs -Ddfs.checksum.combine.mode=COMPOSITE_CRC -checksum " + hdfs_filepath
    res = subprocess.run(cmd_hdfs_chk, shell=True, check=True, capture_output=True, text=True)
    res = res.stdout
    prefix = hdfs_filepath + "\t" + "COMPOSITE-CRC32C\t"
    return res[len(prefix):].rstrip("\n")


def sync_local_hdfs(lc, hdfs, loc, rem):
    print("Synching hdfs and local")
    loc_chk = lc.get_loc_chk(loc)
    hdfs_chk = lc.get_hdfs_chk(loc)
    print("Loc chk: " + asstr(loc_chk))
    print("HDFS chk: " + asstr(hdfs_chk))
    if loc_chk == hdfs_chk:
        print("sameeeeeee")
    else:
        print("diiiiiif")


if __name__ == '__main__':  # for tests
    # cmd = customize_path('/home/athina/hadoop-3.2.1', 'bin/hdfs') + " dfs -ls /"
    # subprocess.run(cmd, shell=True, check=True)
    hdfs_file_checksum('/home/athina/hadoop-3.2.1', '/mrbox/test_input.txt', 'file')

