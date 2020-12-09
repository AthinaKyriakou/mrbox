# class representing a file that is created or will be created locally
import os
import stat
from utils.file_util import to_link


class MRBoxObj:
    def __init__(self, local_path, local_file_size_limit, remote_path=None, remote_file_size=0, remote_file_type=None):
        self.remotePath = remote_path
        self.localPath = local_path
        self.localFileLimit = local_file_size_limit
        self.remoteFileSize = remote_file_size
        self.existsLoc = os.path.exists(local_path)
        self.remoteType = remote_file_type

        if self.existsLoc and os.path.splitext(local_path)[1] == '.link':
            self.localType = 'link'
        elif not self.existsLoc and self.remoteFileSize > self.localFileLimit:
            self.localType = 'link'
            self.localPath = to_link(self.localPath, self.localType)
            self.remoteType = 'file'  # todo: SOS if locally is link remotely can only be file
        elif self.existsLoc and os.path.isdir(local_path):
            self.localType = 'dir'
        elif self.existsLoc and os.path.isfile(local_path):
            self.localType = 'file'
        else:
            self.localType = self.remoteType
        print("Exists loc: " + str(self.existsLoc) + ", remote file type: " + str(self.remoteType))

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

    def create_loc_link(self):
        with open(self.localPath, 'w+') as fp:
            fp.write(self.remotePath)  # todo: add correct link to be opened in browser
        os.chmod(self.localPath, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    def replace_loc_content(self, new_remote_path):
        """
        If the mrbox obj is link, replace its content with new_remote_path
        """
        if self.is_link():
            # os.chmod(self.localPath, stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
            # os.chmod(self.localPath, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            os.chmod(self.localPath, stat.S_IWUSR)
            with open(self.localPath, 'w') as fp:
                fp.write(new_remote_path)  # todo: add correct link to be opened in browser
            os.chmod(self.localPath, stat.S_IRUSR)
