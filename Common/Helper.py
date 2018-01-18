import os, errno

# taken from https://stackoverflow.com/questions/10840533/most-pythonic-way-to-delete-a-file-which-may-not-exist
def remove_file(filename):
    try:
        os.remove(filename)
    except OSError as e:  # this would be "except OSError, e:" before Python 2.6
        print(e)
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise