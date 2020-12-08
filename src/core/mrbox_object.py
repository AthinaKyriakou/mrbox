# class representing a file that is created or will be created locally
from os import path


class MRBoxObj:
    def __init__(self, local_path, local_file_size_limit, remote_path, remote_file_size=0, remote_file_type=None):
        self.remotePath = remote_path
        self.localPath = local_path
        self.localFileLimit = local_file_size_limit
        self.remoteFileSize = remote_file_size
        self.existsLoc = path.exists(local_path)

        if (self.existsLoc and path.splitext(local_path)[1] == '.link') or \
                (not self.existsLoc and remote_file_size > local_file_size_limit):
            self.localType = 'link'
        elif self.existsLoc and path.isdir(local_path):
            self.localType = 'dir'
        elif self.existsLoc and path.isfile(local_path):
            self.localType = 'file'
        else:
            self.localType = remote_file_type

    def is_dir(self):
        return self.localType == 'dir'

    def is_file(self):
        return self.localType == 'file'

    def is_link(self):
        return self.localType == 'link'