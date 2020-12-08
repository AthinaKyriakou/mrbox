import subprocess
from utils.path_util import customize_path, remove_prefix


def hdfs_file_size(hadoop_path, hdfs_filepath):  # todo: how to handle dirs
    """
    Returns the size of a hadoop file in bytes
    :param hadoop_path:
    :param hdfs_filepath:
    :return:
    """
    cmd_hdfs_file_size = customize_path(hadoop_path, 'bin/hdfs') + " dfs -ls " + hdfs_filepath
    res = subprocess.run(cmd_hdfs_file_size, shell=True, check=True, capture_output=True, text=True)
    res = res.stdout
    print("HDFS file size res: " + res)
    file_size = res.split()[4]
    print("HDFS file size in bytes: " + file_size)
    return int(file_size)


def hdfs_file_checksum(hadoop_path, hdfs_filepath, ftype):
    """
    Computes the checksum of a file on hdfs
    :param hadoop_path: where hadoop is installed locally
    :param hdfs_filepath: the path of the file on hdfs
    :param ftype: the type of the local copy of the file ('dir', 'file', 'link')
    :return:
    """
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

