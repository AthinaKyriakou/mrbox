import os


def customize_path(folder_path, filename):
    return os.path.join(folder_path, filename)


def remove_prefix(prefix, filepath):
    prefix = customize_path(prefix, '')
    if filepath.startswith(prefix):
        return filepath[len(prefix):]
    return filepath
