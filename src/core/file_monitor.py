import os
import yaml
import errno
import subprocess
from watchdog.events import FileSystemEventHandler
from utils.path_util import customize_path, remove_prefix
from utils.hdfs_util import hdfs_file_checksum, compare_local_hdfs_copy
from utils.file_util import crc32c_file_checksum, rm_link_extension, to_link
from core.mrbox_object import MRBoxObj


class Event(FileSystemEventHandler):
    def __init__(self, local_dir, hadoop, lc):
        self.local = local_dir
        self.hadoop = hadoop
        self.lc = lc

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
            input_path = customize_path(self.local.localPath, data.get('input'))
            print("input_path: " + input_path)
            output_path = data.get('output')

        # check if the files exists locally
        for f in [mapper_path, reducer_path, input_path]:
            if not os.path.exists(f):
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), f)

        # to issue MR job, the input file should be on hdfs --> need to get the remote path
        hdfs_input_path = self.lc.get_remote_file_path(customize_path(self.local.localPath, input_path))
        print("hdfs_input_path: " + hdfs_input_path)

        # need to generate local + remote output paths
        local_output_path = customize_path(self.local.localPath, output_path)
        hdfs_output_path = customize_path(self.local.remotePath, output_path)

        # issue MR job
        cmd_mr = customize_path(self.hadoop.hadoopPath, 'bin/hadoop') + " jar " \
                 + customize_path(self.hadoop.hadoopPath, 'share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar') \
                 + " -files " + mapper_path + "," + reducer_path + " -mapper 'mapper.py'" + " -reducer 'reducer.py'" \
                 + " -input " + hdfs_input_path + " -output " + hdfs_output_path

        try:
            output_dir = MRBoxObj(local_output_path, self.local.localFileLimit, hdfs_output_path,
                                  remote_file_type='dir')
            self.hadoop.create_locally_synced_dir(cmd_mr, self.lc, output_dir)
        except subprocess.CalledProcessError as e:
            print("Map-Reduce job failed!")
            print(e.output)

    def on_any_event(self, event):
        print(event.event_type, event.src_path)

    def on_modified(self, event):
        print("on_modified")

        # no action if dir or link modification
        obj = MRBoxObj(event.src_path, self.local.localFileLimit, self.lc.get_remote_file_path(event.src_path))
        if obj.is_dir() or obj.is_link():
            return

        # if file, update the local checksum
        loc_chk = crc32c_file_checksum(obj.localPath, obj.localType)
        self.lc.update_tuple_local(obj.localPath, loc_chk)

        # update the copy on HDFS + the hdfs checksum
        try:
            self.hadoop.rm(obj.remotePath)
            self.hadoop.put(obj.localPath, obj.remotePath)
            hdfs_chk = hdfs_file_checksum(self.hadoop.hadoopPath, obj.remotePath, obj.localType)
            self.lc.update_tuple_hdfs(obj.localPath, hdfs_chk)
        except:
            print("HDFS operation to update modified file failed!")

        # compare_local_hdfs_copy(self.lc, event.src_path)

    def on_created(self, event):
        """ Creates dir / file on HDFS & adds mapping with mapping between local + hdfs path in the local db
        If created file is .yaml issues a MR job"""
        print("on_created")

        if self.lc.check_local_path_exists(event.src_path):
            print("file/dir already exists on hdfs - mapped on db")
            remote_file_path = self.lc.get_remote_file_path(event.src_path)
            obj = MRBoxObj(event.src_path, self.local.localFileLimit, remote_file_path)  # do we want remote file size?
            # obj.file_info()
            # update needed to insert the loc_chk in existent db record
            # in case of link: loc_chk != hdfs_chk
            loc_chk = crc32c_file_checksum(obj.localPath, obj.localType)
            self.lc.update_tuple_local(obj.localPath, loc_chk)
        else:
            print("file/dir needs to be created on hdfs - not mapped on db")
            filename = remove_prefix(self.local.localPath, event.src_path)
            remote_file_path = customize_path(self.local.remotePath, filename)
            obj = MRBoxObj(event.src_path, self.local.localFileLimit, remote_file_path)
            # obj.file_info()
            loc_chk = crc32c_file_checksum(obj.localPath, obj.localType)
            self.lc.insert_tuple_local(obj.localPath, obj.remotePath, loc_chk, obj.localType)

        if not self.hadoop.exists(remote_file_path) and obj.is_dir():
            print("creating dir on hdfs")
            self.hadoop.mkdir(remote_file_path)
            hdfs_chk = hdfs_file_checksum(self.hadoop.hadoopPath, obj.remotePath, obj.localType)
            self.lc.update_tuple_hdfs(obj.localPath, hdfs_chk)

        if not self.hadoop.exists(remote_file_path) and obj.is_file():
            print("creating file on hdfs")
            self.hadoop.put(obj.localPath, obj.remotePath)
            hdfs_chk = hdfs_file_checksum(self.hadoop.hadoopPath, obj.remotePath, obj.localType)
            self.lc.update_tuple_hdfs(obj.localPath, hdfs_chk)

        # if it is a link, it already exists

        # compare_local_hdfs_copy(self.lc, event.src_path)

        if obj.is_file() and event.src_path.endswith('.yaml'):
            self.issue_mr_job(obj.localPath)

    def on_deleted(self, event):
        """ Deletes dir / file from HDFS & removes the corresponding tuple from the local db
        In case of a non-empty dir, the db records of its files and subdirectories are also deleted
        the dir file structure does not exist locally anymore
        need to find it through calls to HDFS to delete all related db records"""
        print("on_deleted")
        remote_path = self.lc.get_remote_file_path(event.src_path)
        if event.is_directory:
            list_of_paths = self.hadoop.find_remote_paths(remote_path)
            self.lc.delete_by_remote_path(list_of_paths)
        else:
            self.lc.delete_by_local_path([event.src_path])
        self.hadoop.rm(remote_path)

    def on_moved(self, event):
        """ for non-empty dir: the db records of its files and subdirectories are also modified
        the dir file structure does not exist locally anymore so need to find it through calls to HDFS to update all
        related db records (local + remote path)
        for link: the corresponding remote file should be moved + the path in the link file should be changed
        """
        print("on_moved")
        try:
            rem_src_path = self.lc.get_remote_file_path(event.src_path)
            tmp = customize_path(self.local.remotePath, remove_prefix(self.local.localPath, event.dest_path))
            rem_dest_path = rm_link_extension(tmp)
            existing_main_obj = MRBoxObj(event.dest_path, self.local.localFileLimit, rem_src_path)
            if rem_src_path is not None:

                if existing_main_obj.is_dir():
                    # get all the remote paths of the files in the dir
                    remote_src_paths = self.hadoop.find_remote_paths(rem_src_path)
                    local_remote_tuples = []
                    for rp in remote_src_paths:
                        if rp == rem_src_path:
                            local_remote_tuples.append((rp, event.dest_path, rem_dest_path))
                        else:
                            file_hierarchy = remove_prefix(rem_src_path, rp)
                            new_remote_path = customize_path(rem_dest_path, file_hierarchy)
                            loc_type = self.lc.get_loc_type_by_remote_path(rp)
                            new_local_path = to_link(customize_path(event.dest_path, file_hierarchy), loc_type)
                            local_remote_tuples.append((rp, new_local_path, new_remote_path))
                            # modify links' content
                            existing_obj = MRBoxObj(new_local_path, self.local.localFileLimit, rp)
                            existing_obj.replace_loc_content(new_remote_path)
                    self.lc.update_by_remote_path(local_remote_tuples)

                else:
                    existing_main_obj.replace_loc_content(rem_dest_path)
                    self.lc.update_by_remote_path([(rem_src_path, event.dest_path, rem_dest_path)])

                self.hadoop.mv(rem_src_path, rem_dest_path)
        except FileNotFoundError:
            print("Move already handled!")
