import time
import logging
import os
import configparser
import sys
from watchdog.observers import Observer
from hdfs3 import HDFileSystem
from core.local_catalogue import LocalCatalogue
from core.hadoop_interface import HadoopInterface
from utils.path_util import customize_path
from utils.file_util import bytes_to_mb
from core.file_monitor import Event
from core.mrbox_object import MRBoxObj

# TODO: create properties obj for hardcoded parameters
# one thread to observe and more serialized to do the changes
# start with one thread

if __name__ == '__main__':

    app_name = "mrbox"
    config_file = app_name + ".conf"
    config_folder = os.path.dirname(os.path.realpath(__file__))
    config_filepath = os.path.join(config_folder, config_file)

    # check if a configuration file exists in current path
    if not os.path.exists(config_filepath):
        print("No .conf file is found in %s." % config_folder)
        sys.exit(1)

    # read from mrbox.conf
    config = configparser.ConfigParser()
    config.read(config_filepath)

    # local folder properties
    local_folder = customize_path(config['User']['localPath'], 'mrbox')
    local_file_size_limit_MB = config['User']['localFileSizeMB']
    remote_folder = customize_path(config['User']['hdfsPath'], 'mrbox')
    if not os.path.exists(local_folder):
        os.mkdir(local_folder)
    local_file_size_limit_bytes = bytes_to_mb(int(local_file_size_limit_MB))
    local = MRBoxObj(local_folder, local_file_size_limit_bytes, remote_folder)

    # connect to hdfs and create hadoop interface, todo: check how to create list of multiple hadoops
    hdfs_con = HDFileSystem(host=config['User']['hdfsHost'], port=config['User'].getint('hdfsPort'))
    hadoop_path = config['User']['hadoopPath']
    hdfs_con.mkdir(remote_folder)
    hadoop = HadoopInterface(hdfs_con, hadoop_path)

    # create sqlite db
    full_db_path = os.path.join(config['User']['localPath'], config['User']['dbFile'])
    lc = LocalCatalogue(full_db_path)

    # todo: run sync thread for initial consistency with local folder

    # create thread to monitor /mrbox directory and log events generated
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    event_handler = Event(local, hadoop, lc)
    observer = Observer()
    observer.schedule(event_handler, local_folder, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
