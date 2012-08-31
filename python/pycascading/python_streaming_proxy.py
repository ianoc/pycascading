#!/usr/bin/env python2.6

import sys
import site
import string
import os
import tempfile
import os.path
import time


stdin = sys.stdin
stdout = sys.stdout
sys.stdin = None
sys.stdout = sys.stderr


JYTHON_PATH = os.environ["JYTHONPATH"]
for p in JYTHON_PATH.split(":"):
    if not p.endswith("jar"):
        sys.path.extend(p)

import new
lockModule = new.module("LockFile")
sys.modules["LockFile"] = lockModule
class LockFile(object):
    def __init__(self, lock_file):
        self.__lockfile = lock_file
    def __enter__(self):
        self.__file = tempfile.NamedTemporaryFile("wb")
        self.__name = self.__file.name
        for attempt_number in range(70): # loop aroud a few times
            try:
                os.link(self.__name, self.__lockfile )
                return self
            except OSError as e:
                if time.time() - os.path.getmtime(self.__lockfile) > 600: # No one can lock for more than 10mins
                    try:
                        os.unlink(self.__lockfile)
                    except e:
                        pass
                else:
                    time.sleep(10)
        raise Exception("Locking failed")
    def __exit__(self, type, value, tb):
        self.__file.close()
        os.unlink(self.__lockfile)

lockModule.LockFile = LockFile

def install_with_easy_install(packages):
    with LockFile("/tmp/easy_install_ops3.lock"):
        import subprocess
        subprocess.check_call("sudo easy_install-2.6 -U distribute", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for pkg in packages:
            subprocess.check_call("sudo easy_install-2.6 -U %s" % (pkg), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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

def get_user_function(function_path):
    def full_path_to_function_module(function_module_path):
        last_dot = string.rfind(function_module_path, ".")
        module = function_module_path[0: last_dot]
        first_dot = string.find(module, ".")
        if first_dot < 0:
            # Then we are of the form module.function at the top level, so first dot = last dot
            first_dot = last_dot
        function_path = function_module_path[first_dot +1:]
        return (module, function_path)
    
    module_str,function_path = full_path_to_function_module(function_path)
    user_module = __import__(module_str)
    current_location = user_module
    for next_target in function_path.split("."):
        current_location = getattr(current_location, next_target)
    return current_location


user_function = get_user_function(args.function)

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
line = stdin.readline()
while len(line) > 0:
    # remove leading and trailing whitespace
    line = line.strip()
    tuple = Tuple(json.loads(line))
    for result in user_function(tuple):
        res = json.dumps(result) 
        stdout.write(res)
        stdout.write('\n')
    stdout.write('\n') # Seperator so parent knows we moved onto next tuple
    stdout.flush()
    line = stdin.readline()
