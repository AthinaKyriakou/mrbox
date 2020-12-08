# class representing a file that is created or will be created locally
from os import path


class MRBoxObj:
    def __init__(self, local_path, local_file_size_limit, remote_path, remote_file_size=0, remote_file_type=None):
        self.remotePath = remote_path
        self.localPath = local_path
        self.localFileLimit = local_file_size_limit
        self.remoteFileSize = remote_file_size
        self.existsLoc = path.exists(local_path)
        self.remoteType = remote_file_type

        print("Exists loc: " + str(self.existsLoc) + ", remote file type: " + str(self.remoteType))

        if (self.existsLoc and path.splitext(local_path)[1] == '.link') or \
                (not self.existsLoc and self.remoteFileSize > self.localFileLimit):
            self.localType = 'link'
        elif self.existsLoc and path.isdir(local_path):
            self.localType = 'dir'
        elif self.existsLoc and path.isfile(local_path):
            self.localType = 'file'
        else:
            self.localType = self.remoteType

    def is_dir(self):
        return self.localType == 'dir'

    def is_file(self):
        return self.localType == 'file'

    def is_link(self):
        return self.localType == 'link'

    def file_info(self):
        info = {
            "remotePath": self.remotePath,
            "localPath": self.localPath,
            "localFileLimit": self.localFileLimit,
            "remoteFileSize": self.remoteFileSize,
            "existsLoc": self.existsLoc,
            "remoteType": self.remoteType,
            "localType": self.localType
        }
        print(info)

