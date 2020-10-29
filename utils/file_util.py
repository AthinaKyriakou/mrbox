import crc32c


def crc32c_file_checksum(filepath):
    """
    Calculates the CRC32C checksum of a file locally
    :param filename: local absolute filepath
    :return:
    """
    buf = open(filepath, 'rb').read()
    buf = (crc32c.crc32c(buf) & 0xFFFFFFFF)
    return "%08x" % buf


if __name__ == '__main__':  # for tests
    print(crc32c_file_checksum('/home/athina/Desktop/praktiki/mrbox/test_input.txt'))
