import time
import logging
import os
import configparser
import sys
from watchdog.observers import Observer
from hdfs3 import HDFileSystem
from core.local_catalogue import LocalCatalogue
from utils.path_util import customize_path
from core.file_monitor import Event

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

    local_folder = customize_path(config['User']['localPath'], 'mrbox')
    remote_folder = customize_path(config['User']['hdfsPath'], 'mrbox')
    if not os.path.exists(local_folder):
        os.mkdir(local_folder)

    # connect to hdfs
    hdfs = HDFileSystem(host=config['User']['hdfsHost'], port=config['User'].getint('hdfsPort'))
    hdfs.mkdir(remote_folder)

    # create sqlite db
    full_db_path = os.path.join(local_folder, config['User']['dbFile'])
    lc = LocalCatalogue(full_db_path)

    hadoop_path = config['User']['hadoopPath']

    # list with tuples of (local_path, remote_path) of files that are not synced between local + hdfs dir
    files_to_sync = []

    # monitor /mrbox directory and log events generated
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    event_handler = Event(hdfs, lc, hadoop_path, local_folder, remote_folder, files_to_sync)
    observer = Observer()
    observer.schedule(event_handler, local_folder, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
