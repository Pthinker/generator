#-*- coding: utf-8 -*-
from decimal import Decimal

def is_numeric(a, none_is_ok=True):
    if isinstance(a, int) or isinstance(a, float) or isinstance(a, long) or isinstance(a, Decimal) or (a is None and none_is_ok):
        return True
    return False

def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def log_traceback(logger, traceback):
    lines = traceback.format_exc().splitlines()
    for line in lines:
        logger.error(line)

def partial_dict_index(dicts, dict_part):
    for i, current_dict in enumerate(dicts):
        # if this dict has all keys required and the values match
        if all(key in current_dict and current_dict[key] == val
                for key, val in dict_part.items()):
            return i
    return None
