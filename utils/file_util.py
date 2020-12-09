import crc32c
import os

# todo: change str to globals taken from mrbox_obj


def crc32c_file_checksum(filepath, ftype):
    """
    Calculates the CRC32C checksum of a file locally
    :param ftype: 'dir' or 'file'
    :param filepath: local absolute filepath
    :return:
    """
    if ftype == 'dir':
        return None
    buf = open(filepath, 'rb').read()
    ret = "%08x" % (crc32c.crc32c(buf) & 0xFFFFFFFF)
    return ret


def bytes_to_mb(file_size_bytes):
    res = int(file_size_bytes*1024*1024)
    return res


def to_link(filename, loc_type):
    if loc_type == 'link':
        return filename + '.link'
    else:
        return filename


def rm_link_extension(path):
    if os.path.splitext(path)[1] == '.link':
        return os.path.splitext(path)[0]
    else:
        return path


if __name__ == '__main__':  # for tests
    print(crc32c_file_checksum('/home/athina/Desktop/praktiki/mrbox/mrbox/test_input.txt', 'file'))
