import time
import logging
import os
import yaml
import errno
import subprocess
import argparse
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from hdfs3 import HDFileSystem
from core.local_catalogue import LocalCatalogue
from utils.path_util import customize_path, remove_prefix
from utils.hdfs_util import find_remote_paths, create_locally_synced_hdfs_dir, hdfs_file_checksum, sync_local_hdfs
from utils.file_util import crc32c_file_checksum


def issue_mr_job(filepath):
    """
    Called when a .yaml file is created.
    Reads the paths of mapper, reducer, input dir, output dir + checks that they exist locally + remotely.
    Issues the MR job.
    :param filepath: the path of the created yaml file, all specified paths are local
    :return:
    """
    print("issue_mr_job")

    global lc, hdfs, hadoopPath, localFolder, remoteFolder

    with open(filepath, 'r') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        mapper_path = data.get('mapper')
        reducer_path = data.get('reducer')
        input_path = data.get('input')
        output_path = data.get('output')

    # check if the files exist locally
    for f in [mapper_path, reducer_path, input_path]:
        if not os.path.exists(f):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), f)

    # to issue MR job, the input file should be on hdfs --> need to get the remote path
    hdfs_input_path = lc.get_remote_file_path(customize_path(localFolder, input_path))

    # need to generate local + remote output paths
    local_output_path = customize_path(localFolder, output_path)
    hdfs_output_path = customize_path(remoteFolder, output_path)

    # issue MR job
    cmd_mr = customize_path(hadoopPath, 'bin/hadoop') + " jar " + \
             customize_path(hadoopPath, 'share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar') + " -files " + mapper_path\
             + "," + reducer_path + " -mapper 'mapper.py'" + " -reducer 'reducer.py'" + " -input " + hdfs_input_path + \
             " -output " + hdfs_output_path

    try:
        create_locally_synced_hdfs_dir(cmd_mr, hdfs, lc, local_output_path, hdfs_output_path, hadoopPath)
    except subprocess.CalledProcessError as e:
        print("Map-Reduce job failed!")
        print(e.output)


class Event(FileSystemEventHandler):
    def on_any_event(self, event):
        print(event.event_type, event.src_path)

    def on_modified(self, event):
        # Replaces the updated file on HDFS and updates the local db 
        global hdfs, lc
        print("on_modified")

        if event.is_directory:
            return

        remote_file_path = lc.get_remote_file_path(event.src_path)
        hdfs.rm(remote_file_path)
        hdfs.put(event.src_path, remote_file_path)
        lc.set_modified_local(event.src_path)  # todo: implement in_sync logic

    def on_created(self, event):
        """ Creates dir / file on HDFS & adds mapping with mapping between local + hdfs path in the local db
        If created file is .yaml issues a MR job"""
        global hdfs, lc, localFolder, remoteFolder, hadoopPath, files_to_sync
        print("on_created")

        if event.is_directory:
            ftype = 'dir'
        else:
            ftype = 'file'

        filename = remove_prefix(localFolder, event.src_path)
        remote_file_path = customize_path(remoteFolder, filename)

        loc_chk = crc32c_file_checksum(event.src_path, ftype)  # todo: need to implement dir checksum
        if lc.check_local_path_exists(event.src_path):
            lc.update_tuple_local(event.src_path, loc_chk)
        else:
            lc.insert_tuple_local(event.src_path, remote_file_path, loc_chk)

        if not hdfs.exists(remote_file_path):
            if event.is_directory:  # todo: need to implement dir checksum
                hdfs.mkdir(remote_file_path)
                hdfs_chk = hdfs_file_checksum(hadoopPath, remote_file_path, 'dir')
            if not event.is_directory:
                hdfs.put(event.src_path, remote_file_path)
                hdfs_chk = hdfs_file_checksum(hadoopPath, remote_file_path, 'file')
            lc.update_tuple_hdfs(event.src_path, hdfs_chk)

        # compare checksums: if same do nothing, if dif keep most recent in local + hdfs the most recent copy
        sync_local_hdfs(lc, hdfs, event.src_path, remote_file_path)

        if not event.is_directory and event.src_path.endswith('.yaml'):
            issue_mr_job(event.src_path)

    def on_deleted(self, event):
        """ Deletes dir / file from HDFS & removes the corresponding tuple from the local db
        In case of a non-empty dir, the db records of its files and subdirectories are also deleted
        the dir file structure does not exist locally anymore
        need to find it through calls to HDFS to delete all related db records"""
        global hdfs, lc
        print("on_deleted")
        remote_path = lc.get_remote_file_path(event.src_path)
        if event.is_directory:
            list_of_paths = find_remote_paths(remote_path, hdfs)
            lc.delete_by_remote_path(list_of_paths)
        else:
            lc.delete_by_local_path([event.src_path])
        hdfs.rm(remote_path)

    def on_moved(self, event):
        """ In case of a non-empty dir, the db records of its files and subdirectories are also modified
        the dir file structure does not exist locally anymore
        need to find it through calls to HDFS to update all related db records (local + remote path) """
        global hdfs, lc, localFolder, remoteFolder
        print("on_moved")
        try:
            remote_src_path = lc.get_remote_file_path(event.src_path)
            tmp = remove_prefix(localFolder, event.dest_path)
            remote_dest_path = customize_path(remoteFolder, tmp)
            if remote_src_path is not None:
                if event.is_directory:
                    remote_src_paths = find_remote_paths(remote_src_path, hdfs)
                    local_remote_tuples = []
                    for rp in remote_src_paths:
                        if rp == remote_src_path:
                            local_remote_tuples.append((rp, event.dest_path, remote_dest_path))
                        else:
                            file_hierarchy = remove_prefix(remote_src_path, rp)
                            new_local_path = customize_path(event.dest_path, file_hierarchy)
                            new_remote_path = customize_path(remote_dest_path, file_hierarchy)
                            local_remote_tuples.append((rp, new_local_path, new_remote_path))
                    lc.update_by_remote_path(local_remote_tuples)
                else:
                    lc.update_by_remote_path([(remote_src_path, event.dest_path, remote_dest_path)])
                hdfs.mv(remote_src_path, remote_dest_path)
        except FileNotFoundError:
            print("Move already handled!")


# TODO: create properties obj for hardcoded parameters
# one thread to observe and more serialized to do the changes
# start with one thread
if __name__ == '__main__':

    files_to_sync = []

    parser = argparse.ArgumentParser(description='Configuring the mrbox app...')
    parser.add_argument('--localPath', type=str, help='local path of app')
    parser.add_argument('--dbFile', default='mrbox.db', type=str, help='sqlite db file')
    parser.add_argument('--hdfsPath', type=str, default='/', help='hdfs path')
    parser.add_argument('--hdfsHost', type=str, help='hdfs host')
    parser.add_argument('--hdfsPort', default=9000, type=int, help='hdfs port')
    parser.add_argument('--hadoopPath', type=str, help='hadoop local installation path')
    args = parser.parse_args()

    localFolder = customize_path(args.localPath, 'mrbox')
    remoteFolder = customize_path(args.hdfsPath, 'mrbox')
    if not os.path.exists(localFolder):
        os.mkdir(localFolder)

    # todo: create a retry strategy
    hdfs = HDFileSystem(host=args.hdfsHost, port=args.hdfsPort)
    hdfs.mkdir(remoteFolder)

    # create sqlite db
    lc = LocalCatalogue(args.dbFile)

    hadoopPath = args.hadoopPath

    # monitor /mrbox directory and log events generated
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    path = localFolder
    event_handler = Event()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
