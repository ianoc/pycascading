#!/usr/bin/env python2.6

import sys
import site
import string
import os,pprint

JYTHON_PATH = os.environ["JYTHONPATH"]
for p in JYTHON_PATH.split(":"):
    if not p.endswith("jar"):
        sys.path.extend(p)

def install_with_easy_install(packages):
    import subprocess
    subprocess.check_call(["sudo", "easy_install-2.6", "-U", "distribute"])
    for pkg in packages: 
        subprocess.check_call(["sudo", "easy_install-2.6", pkg])
        reload(site) # Reload often incase more downstream needs it

try:
    import argparse
    import simplejson as json
except ImportError, e:
    install_with_easy_install(["argparse", "simplejson"])
    import argparse
    import simplejson as json


parser = argparse.ArgumentParser(description='Handles dependency fetching and setting up of the python env, handling the streaming portion of the op')
parser.add_argument('function', help='Function to call, must be in the PYTHONPATH')
parser.add_argument('userlibs', nargs='*',
                   help='User Libraries')
args = parser.parse_args()
needed_libs = []
for lib in args.userlibs:
    try:
        __import__(lib)
    except ImportError, e:
        needed_libs.append(lib)

if len(needed_libs) > 0:
    install_with_easy_install(needed_libs)

def full_path_to_function_module(function_module_path):
    last_dot = string.rfind(function_module_path, ".")
    module = function_module_path[0: last_dot]
    function = function_module_path[last_dot +1:]
    return (module, function)

module_str,function_str = full_path_to_function_module(args.function)
user_module = __import__(module_str)
user_function = getattr(user_module, function_str)

base_fields = None
class Tuple(object):
    def __init__(self, args):
        self.__args = args
    def get(self,indx):
        return self.__args[indx]
    def size(self):
        return len(self.__args)
    def __len__(self):
        return len(self.__args)
    def __iter__(self):
        return self.__args.__iter__()

# input comes from STDIN (standard input)
line = sys.stdin.readline()
while len(line) > 0:
    # remove leading and trailing whitespace
    line = line.strip()
    tuple = Tuple(json.loads(line))
    for result in user_function(tuple):
        res = json.dumps(result) 
        print(res)
    print '' # Seperator so parent knows we moved onto next tuple
    line = sys.stdin.readline()
