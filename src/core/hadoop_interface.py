import os


class HadoopInterface:
    def __init__(self, hdfs):
        self.hdfs = hdfs

    def get(self, mrboxf):  # triggers on_created()
        if mrboxf.type == 'link':
            with open(mrboxf.local_path, 'w+') as fp:
                fp.write(mrboxf.remote_path)  # add correct link to be opened in browser
        else:
            self.hdfs.get(mrboxf.remote_path, mrboxf.local_path)





