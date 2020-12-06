# class representing a file that is created or will be created locally
from os import path


class MRBoxFile:
    def __init__(self, remote_source, remote_path, remote_file_size, local_path, local_file_size_limit,
                 remote_file_type=None):
        self.remoteSource = remote_source
        self.remotePath = remote_path
        self.localPath = local_path
        self.localFileLimit = local_file_size_limit
        self.remoteFileSize = remote_file_size
        self.existsLoc = path.exists(local_path)

        if (self.existsLoc and path.splitext(local_path)[1] == '.link') or \
                (not self.existsLoc and remote_file_size > local_file_size_limit):
            self.type = 'link'
        elif self.existsLoc and path.isdir(local_path):
            self.type = 'dir'
        elif self.existsLoc and path.isfile(local_path):
            self.type = 'file'
        else:
            self.type = remote_file_type
