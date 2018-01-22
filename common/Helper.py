import os, errno, zipfile, fnmatch
from common.Constants import DOWNLOAD_FOLDER


def remove_file(filename):
    # taken from https://stackoverflow.com/questions/10840533/most-pythonic-way-to-delete-a-file-which-may-not-exist
    try:
        os.remove(filename)
    except OSError as e:  # this would be "except OSError, e:" before Python 2.6
        print(e)
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise


def unzip(path, filename):
    # taken from https://stackoverflow.com/questions/3451111/unzipping-files-in-python
    path_with_filename = DOWNLOAD_FOLDER + path + filename

    with zipfile.ZipFile(path_with_filename, "r") as zip_ref:
        directory, separator, extension = filename.rpartition('.')
        path_to_files = DOWNLOAD_FOLDER + path + directory

        zip_ref.extractall(path_to_files)
        return path_to_files


def find(pattern, path):
    # taken from https://stackoverflow.com/questions/1724693/find-a-file-in-python
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))

    # return the first result
    return result.pop(0)
