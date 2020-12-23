import os
import configparser
import sys
from hdfs3 import HDFileSystem
from core.local_catalogue import LocalCatalog
from core.hadoop_interface import HadoopInterface
from utils.path_util import customize_path

SUPPORTED_LINK_CMDS = ['head', 'tail']


def main(argv):
    app_name = "mrbox"
    config_file = app_name + ".conf"
    config_folder = os.path.dirname(os.path.realpath(__file__))
    config_filepath = os.path.join(config_folder, config_file)

    # check if a configuration file exists in current path
    if not os.path.exists(config_filepath):
        print("No .conf file is found in %s." % config_folder)
        sys.exit(1)

    # parse arguments
    if len(argv) == 0 or len(argv) > 2:
        print("Wrong number of operands.\nTry 'mrview.py help' for more information.")
        sys.exit(1)
    elif argv[0] == 'help':
        print("mrview.py cmd absolute_file_path")
        print("Supported link commands: ", *SUPPORTED_LINK_CMDS, sep=',')
        sys.exit(1)

    cmd = argv[0]
    file_path = argv[1]

    # read from mrbox.conf
    config = configparser.ConfigParser()
    config.read(config_filepath)

    local_folder = customize_path(config['User']['localPath'], 'mrbox')
    local_path = customize_path(local_folder, file_path)

    if not os.path.exists(local_path):
        print("File does not exist in ", local_folder)
        sys.exit(1)

    # connect to hdfs and create hadoop interface
    hdfs_con = HDFileSystem(host=config['User']['hdfsHost'], port=config['User'].getint('hdfsPort'))
    hadoop_path = config['User']['hadoopPath']
    hadoop = HadoopInterface(hdfs_con, hadoop_path)

    # create sqlite db instance
    full_db_path = os.path.join(config['User']['localPath'], config['User']['dbFile'])
    lc = LocalCatalog(full_db_path)

    # need to query db to get type and remote path if link
    hdfs_path = lc.get_remote_file_path(local_path)
    loc_type = lc.get_loc_type_by_remote_path(hdfs_path)

    # if link, only the supported cmds can be executed on HDFS copy
    # if dir or file, UNIX cmds to be executed locally
    if loc_type == 'link':
        if cmd not in SUPPORTED_LINK_CMDS:
            print(cmd, " not supported for links.\nTry 'mrview.py help' for more information.")
            sys.exit(1)
        elif cmd == 'head':
            hadoop.head(hdfs_path)
        elif cmd == 'tail':
            hadoop.tail(hdfs_path)
    else:
        os.system(cmd + ' ' + local_path)


if __name__ == "__main__":
    main(sys.argv[1:])

