import subprocess
import os
from core.mrbox_file import MRBoxFile
from utils.path_util import customize_path, remove_prefix


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


def create_locally_synced_hdfs_dir(cmd, hdfs, lc, loc_path, hdfs_path, hadoop_path, local_file_size_limit):
    """
    Creates a dir on hdfs by running the cmd command and creates a copy of it locally
    :param cmd: bash command to create hdfs dir
    :param hdfs: the connection to HDFS namenode
    :param lc: sqlite3 db class instance
    :param loc_path: the path that dir will be created locally
    :param hdfs_path: the path that dir will be created on hdfs
    :param hadoop_path: local path where hadoop is installed
    :param local_file_size_limit: the maximum size (MB) of a local file synced from HDFS
    :return:
    """

    subprocess.run(cmd, shell=True, check=True)
    hdfs_chk = hdfs_file_checksum(hadoop_path, hdfs_path, 'dir')
    lc.insert_tuple_hdfs(loc_path, hdfs_path, hdfs_chk)
    os.mkdir(loc_path)  # creates an empty directory of hdfs outputs locally, triggers on_created()

    # todo: decide if locally there is a link to the file or the whole file
    # the same is propagated to b2drop
    for rp in hdfs.ls(hdfs_path):
        hdfs_chk = hdfs_file_checksum(hadoop_path, rp, 'file')
        f = remove_prefix(hdfs_path, rp)
        lp = customize_path(loc_path, f)
        file_size = hdfs_file_size(hadoop_path, rp)
        mrbox_file = MRBoxFile(rp, lp, local_file_size_limit, file_size)
        lc.insert_tuple_hdfs(lp, rp, hdfs_chk, mrbox_file.type)

        # get file from hdfs (need to chose if whole file or link)
        if mrbox_file.type == 'file':
            hdfs.get(rp, lp)  # triggers on_created()
        elif mrbox_file.type == 'link':
            print("Creating link to hdfs locally!")


def hdfs_file_size(hadoop_path, hdfs_filepath):  # todo: how to handle dirs
    cmd_hdfs_file_size = customize_path(hadoop_path, 'bin/hdfs') + " dfs -du -h " + hdfs_filepath
    res = subprocess.run(cmd_hdfs_file_size, shell=True, check=True, capture_output=True, text=True)
    res = res.stdout
    file_size = res.split("\t")[0]  # file size in bytes
    file_size_mb = float(file_size) / (1024.0 * 1024.0)
    return round(file_size_mb, 2)  # file size in MB


def hdfs_file_checksum(hadoop_path, hdfs_filepath, ftype):
    if ftype == 'dir':
        return None
    cmd_hdfs_chk = customize_path(hadoop_path, 'bin/hdfs') + \
                   " dfs -Ddfs.checksum.combine.mode=COMPOSITE_CRC -checksum " + hdfs_filepath
    res = subprocess.run(cmd_hdfs_chk, shell=True, check=True, capture_output=True, text=True)
    res = res.stdout
    prefix = hdfs_filepath + "\t" + "COMPOSITE-CRC32C\t"
    return res[len(prefix):].rstrip("\n")


def compare_local_hdfs_copy(lc, loc, files_to_sync):
    loc_chk = lc.get_loc_chk(loc)
    hdfs_chk = lc.get_hdfs_chk(loc)
    if loc_chk == hdfs_chk:
        return
    else:
        print("dif checksums found!")
        files_to_sync.append((loc, lc.get_remote_file_path(loc)))


if __name__ == '__main__':  # for tests
    # cmd = customize_path('/home/athina/hadoop-3.2.1', 'bin/hdfs') + " dfs -ls /"
    # subprocess.run(cmd, shell=True, check=True)
    hdfs_file_checksum('/home/athina/hadoop-3.2.1', '/mrbox/test_input.txt', 'file')

