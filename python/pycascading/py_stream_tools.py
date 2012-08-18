from pycascading.helpers import *
import os, string
from com.xhaus.jyson import JysonCodec

 
class PyMod(object):
    def __init__(self, modName):
        self.modName = modName

@composable
def py_stream_task(*args, **kwargs):
    if "libs" in kwargs:
        libs = kwargs["libs"]
    else:
        libs = []
    op = kwargs["op"]
    """Maps the given input fields to output fields."""
    input_selector = None
    target_module = None
    output_fields = None
    if len(args) == 2:
       (input_selector, target_module, output_fields) = \
       (Fields.ALL, args[1], None)
    elif len(args) == 3:
        if isinstance(args[2], PyMod):
            # parent, [input fields], [PyMod]
            (input_selector, target_module, output_fields) = (args[1], args[2], None)
        else:
            # parent, [PyMod], Output Fields
            (input_selector, target_module, output_fields) = (Fields.ALL, args[1], args[2])
    elif len(args) == 4:
        (input_selector, target_module, output_fields) = (args[1], args[2], args[3])
    else:
       raise Exception('py_stream* needs to be called with 1 to 3 parameters')
    
    if not isinstance(target_module, PyMod):
        raise Exception("Function Modules should be wrapped in PyMod")
    
    function = target_module.modName
    
    parent = args[0]
    
    if output_fields is not None and isinstance(output_fields, str):
        output_fields = [output_fields]

    @udf_map
    def current_proxy_parser(tuple):
        input_str = tuple.get("stream_output")
        yield JysonCodec.loads(input_str)

    if(output_fields is not None):
        current_proxy_parser = current_proxy_parser()
        current_proxy_parser.decorators["produces"] = output_fields
 
    @udf_map(produces="stream_in")
    def toStream(tuple):
        res = []
        for i in range(tuple.size()):
            res.append(tuple.get(i))
        yield [JysonCodec.dumps(res)]


    if not isinstance(libs, list):
        raise Exception("Libs must be a list")
    if len(libs) > 0:
        user_libs = string.join(libs, " ")
    else:
        user_libs = None
    
    if not isinstance(function, str):
        raise Exception("The function must be a fully qualified function name") 

    if op == "to":
        op = map_to
    elif op == "replace":
        op = map_replace
    elif op == "add":
        op = map_add
    cmd_line = ["python", "-u", "${pycascading.root}/python/pycascading/python_streaming_proxy.py", function]
    if user_libs is not None:
        cmd_line.append(user_libs)

    return  parent | op(input_selector, toStream) | \
            stream_replace("stream_in", cmd_line, skipOffset = True ) |\
            map_replace("stream_output", current_proxy_parser)

    
def py_stream_replace(*args, **kwargs):
    kwargs["op"] = "replace"
    return py_stream_task(*args, **kwargs)

def py_stream_to(*args, **kwargs):
    kwargs["op"] = "to"
    return py_stream_task(*args, **kwargs)

def py_stream_add(*args, **kwargs):
    kwargs["op"] = "add"
    return py_stream_task(*args, **kwargs)
