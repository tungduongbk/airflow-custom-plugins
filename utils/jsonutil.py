import re
import json
from typing import Callable, Dict, Union

convert_to_snake_case = lambda name: re.sub(
    '_+', '_', re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)).lower()


def json_flatten(
        data: Union[Dict, str],
        col_normalize: Callable = lambda c: c,
        sep: str = "_"):
    js = json.loads(data) if type(data) is str else data
    record = {}
    def flatten_rec(k, v):
        if type(v) is not dict:
            record[k] = v
        else:
            for sk, sv in v.items():
                flatten_rec(sep.join([col_normalize(k), col_normalize(sk)]), sv)
    for k, v in js.items():
        flatten_rec(k, v)
    return record
