import crc32c

# todo: check if i can merge with mrbox_file


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
    print(res)
    return res


if __name__ == '__main__':  # for tests
    print(crc32c_file_checksum('/home/athina/Desktop/praktiki/mrbox/mrbox/test_input.txt', 'file'))
