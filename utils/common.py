import time
import string
import random
import importlib
from typing import Dict, List
from itertools import islice

def sub_dict(d: Dict, included_keys: List[str] = []) -> Dict:
    return {k:v for k,v in d.items() if k in included_keys}

def println(msg, new_line=1):
    for _ in range(new_line):
        print()
    print(msg)

def chunk(it, size):
    it = iter(it)
    return iter(lambda: list(islice(it, size)), [])


def timing(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        returned_value = func(*args, **kwargs)
        print(f"Took {time.time() - start_time} seconds")
        return returned_value
    return wrapper

def rand_str(length=10):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))

def get_func_from_name(function_string: str):
    mod_name, func_name = function_string.rsplit('.',1)
    mod = importlib.import_module(mod_name)
    func = getattr(mod, func_name)
    return func
