import os
import shutil
from glob import glob

def listdir(path, recursive=False):
    return glob(path, recursive)

def make_new_folder(folder):
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.makedirs(folder)

def clear_fs_obj(obj):
    if os.path.isdir(obj):
        shutil.rmtree(obj)
    else:
        os.remove(obj)
