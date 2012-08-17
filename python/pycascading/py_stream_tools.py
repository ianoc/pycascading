from pycascading.helpers import *
import os, string
from com.xhaus.jyson import JysonCodec as json

 

class StreamTask(MetaChain):
    def __init__(self, replace_which, function, libs = [], output_fields = None, op = stream_replace):
        self.__replace_which = replace_which
        self.__function = function
        self.__libs = libs
        self.__output_fields = output_fields
        self.__op = op

    def proxy(self, parent):
        output_fields = self.__output_fields
        if self.__output_fields is None:
            @udf_map(produces=["stream_out"])
            def current_proxy_parser(tuple):
                input_str = tuple.get("stream_output")
                yield [input_str]
        else:
            @udf_map(produces=["my out"])
            def current_proxy_parser(tuple):
                input_str = tuple.get("stream_output")
                yield json.loads(input_str)
            current_proxy_parser = current_proxy_parser()
            current_proxy_parser.decorators["produces"] = self.__output_fields
     
        @udf_map(produces="stream_in")
        def toStream(tuple):
            yield[json.dumps(list(tuple))]


        if not isinstance(self.__libs, list):
            raise Exception("Libs must be a list")
        user_libs = string.join(self.__libs, " ")
        
        if not isinstance(self.__function, str):
            raise Exception("The function must be a fully qualified function name") 

        return parent | self.__op(self.__replace_which, ["python", "-u", "${pycascading.root}/python/pycascading/python_streaming_proxy.py", self.__function, user_libs ], skipOffset = True ) | map_replace("stream_output", current_proxy_parser)

def py_stream_replace(*args, **kwargs):
    kwargs["op"] = stream_replace
    return StreamTask(*args, **kwargs)