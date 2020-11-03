import os
import yaml
import errno
import subprocess
from watchdog.events import FileSystemEventHandler
from utils.path_util import customize_path, remove_prefix
from utils.hdfs_util import find_remote_paths, create_locally_synced_hdfs_dir, hdfs_file_checksum, \
    compare_local_hdfs_copy
from utils.file_util import crc32c_file_checksum


class Event(FileSystemEventHandler):
    def __init__(self, hdfs, lc, hadoop_path, local_folder, remote_folder, files_to_sync):
        self.hdfs = hdfs
        self.lc = lc
        self.hadoopPath = hadoop_path
        self.localFolder = local_folder
        self.remoteFolder = remote_folder
        self.filesToSync = files_to_sync

    def issue_mr_job(self, filepath):
        """
        Called when a .yaml file is created.
        Reads the paths of mapper, reducer, input dir, output dir + checks that they exist locally + remotely.
        Issues the MR job.
        :param filepath: the path of the created yaml file, all specified paths are local
        :return:
        """
        print("issue_mr_job")

        with open(filepath, 'r') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            mapper_path = data.get('mapper')
            reducer_path = data.get('reducer')
            input_path = os.path.join(self.localFolder, data.get('input'))
            output_path = data.get('output')

        # check if the files exist locally
        for f in [mapper_path, reducer_path, input_path]:
            if not os.path.exists(f):
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), f)

        # to issue MR job, the input file should be on hdfs --> need to get the remote path
        hdfs_input_path = self.lc.get_remote_file_path(customize_path(self.localFolder, input_path))

        # need to generate local + remote output paths
        local_output_path = customize_path(self.localFolder, output_path)
        hdfs_output_path = customize_path(self.remoteFolder, output_path)

        # issue MR job
        cmd_mr = customize_path(self.hadoopPath, 'bin/hadoop') + " jar " \
                 + customize_path(self.hadoopPath, 'share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar') \
                 + " -files " + mapper_path + "," + reducer_path + " -mapper 'mapper.py'" + " -reducer 'reducer.py'" \
                 + " -input " + hdfs_input_path + " -output " + hdfs_output_path

        try:
            create_locally_synced_hdfs_dir(cmd_mr, self.hdfs, self.lc, local_output_path, hdfs_output_path, self.hadoopPath)
        except subprocess.CalledProcessError as e:
            print("Map-Reduce job failed!")
            print(e.output)

    def on_any_event(self, event):
        print(event.event_type, event.src_path)

    def on_modified(self, event):
        # When a file is modified locally
        print("on_modified")

        if event.is_directory:  # todo: implement dir checksum
            return
        self.lc.update_tuple_local(event.src_path, crc32c_file_checksum(event.src_path, 'file'))
        remote_file_path = self.lc.get_remote_file_path(event.src_path)
        try:
            self.hdfs.rm(remote_file_path)
            self.hdfs.put(event.src_path, remote_file_path)
            self.lc.update_tuple_hdfs(event.src_path, hdfs_file_checksum(self.hadoopPath, remote_file_path, 'file'))
        except:  # todo: replace with specific exception once found
            print("HDFS operation failed!")

        compare_local_hdfs_copy(self.lc, event.src_path, self.filesToSync)

    def on_created(self, event):
        """ Creates dir / file on HDFS & adds mapping with mapping between local + hdfs path in the local db
        If created file is .yaml issues a MR job"""
        print("on_created")

        filename = remove_prefix(self.localFolder, event.src_path)
        remote_file_path = customize_path(self.remoteFolder, filename)

        if event.is_directory:
            ftype = 'dir' # todo: need to implement dir checksum
        else:
            ftype = 'file'

        loc_chk = crc32c_file_checksum(event.src_path, ftype)
        if self.lc.check_local_path_exists(event.src_path):
            self.lc.update_tuple_local(event.src_path, loc_chk)
        else:
            self.lc.insert_tuple_local(event.src_path, remote_file_path, loc_chk)

        if not self.hdfs.exists(remote_file_path) and event.is_directory:
            print("creating dir on hdfs")
            try:
                self.hdfs.mkdir(remote_file_path)
            except:
                print("HDFS operation failed!")
        if not self.hdfs.exists(remote_file_path) and not event.is_directory:
            print("creating file on hdfs")
            try:
                self.hdfs.put(event.src_path, remote_file_path)
            except:
                print("HDFS operation failed!")

        hdfs_chk = hdfs_file_checksum(self.hadoopPath, remote_file_path, ftype)
        self.lc.update_tuple_hdfs(event.src_path, hdfs_chk)
        # lc.update_tuple_hdfs(event.src_path, 'aaaaaaaa')  # to test syncing
        compare_local_hdfs_copy(self.lc, event.src_path, self.filesToSync)

        if not event.is_directory and event.src_path.endswith('.yaml'):
            self.issue_mr_job(event.src_path)

    def on_deleted(self, event):
        """ Deletes dir / file from HDFS & removes the corresponding tuple from the local db
        In case of a non-empty dir, the db records of its files and subdirectories are also deleted
        the dir file structure does not exist locally anymore
        need to find it through calls to HDFS to delete all related db records"""
        print("on_deleted")
        remote_path = self.lc.get_remote_file_path(event.src_path)
        if event.is_directory:
            list_of_paths = find_remote_paths(remote_path, self.hdfs)
            self.lc.delete_by_remote_path(list_of_paths)
        else:
            self.lc.delete_by_local_path([event.src_path])
        self.hdfs.rm(remote_path)

    def on_moved(self, event):
        """ In case of a non-empty dir, the db records of its files and subdirectories are also modified
        the dir file structure does not exist locally anymore
        need to find it through calls to HDFS to update all related db records (local + remote path) """
        print("on_moved")
        try:
            remote_src_path = self.lc.get_remote_file_path(event.src_path)
            tmp = remove_prefix(self.localFolder, event.dest_path)
            remote_dest_path = customize_path(self.remoteFolder, tmp)
            if remote_src_path is not None:
                if event.is_directory:
                    remote_src_paths = find_remote_paths(remote_src_path, self.hdfs)
                    local_remote_tuples = []
                    for rp in remote_src_paths:
                        if rp == remote_src_path:
                            local_remote_tuples.append((rp, event.dest_path, remote_dest_path))
                        else:
                            file_hierarchy = remove_prefix(remote_src_path, rp)
                            new_local_path = customize_path(event.dest_path, file_hierarchy)
                            new_remote_path = customize_path(remote_dest_path, file_hierarchy)
                            local_remote_tuples.append((rp, new_local_path, new_remote_path))
                    self.lc.update_by_remote_path(local_remote_tuples)
                else:
                    self.lc.update_by_remote_path([(remote_src_path, event.dest_path, remote_dest_path)])
                self.hdfs.mv(remote_src_path, remote_dest_path)
        except FileNotFoundError:
            print("Move already handled!")
