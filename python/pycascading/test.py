import unittest
import cascading.tuple.Tuple
import os
import tempfile
import shutil
from pycascading.helpers import *

class ProducesOutputMissMatchError(Exception):
    pass

class CascadingTestCase(unittest.TestCase):
    def unwrap_function(self, wrapped_function):
        if isinstance(wrapped_function, DecoratedFunction):
            return wrapped_function.decorators['function']
        else:
            return wrapped_function

    def get_produces(self, wrapped_function):
        if isinstance(wrapped_function, DecoratedFunction):
            return wrapped_function.decorators['produces']
        else:
            return None


    def run_udf(self, inputs, function, error_on_different_length_tuple = False):
        assert(isinstance(inputs, list))
        input_tuples = []
        if (isinstance(inputs[0], list)):
            for raw_tuple in inputs:
                input_tuples.append(cascading.tuple.Tuple(raw_tuple))
        else:
            input_tuples.append(cascading.tuple.Tuple(inputs))

        real_fun = self.unwrap_function(function)
        produces = self.get_produces(function)
        outputs = []
        for input_tuple in input_tuples:
            for output in real_fun(input_tuple):
                if produces is not None:
                    if len(produces) != len(output) and error_on_different_length_tuple:
                        raise ProducesOutputMissMatchError("Error the expected length per tuple didn't match the function output")
                    output = output[0:len(produces)]
                outputs.append(output)
        return outputs

    @staticmethod
    def remap_flow_sources(flow, mapping={}):
        replacement_values = {}
        ret_map = {}
        for key,val in flow.source_map.iteritems():
            scheme = val.getScheme()
            source_path = val.path.toString()
            input_path = None
            if source_path in mapping:
                input_path = mapping[source_path]
            elif '*' in mapping:
                input_path = mapping['*']
            
            if input_path is not None:
                input_tap = Hfs(scheme, input_path)
                replacement_values[key] = input_tap
                ret_map[source_path] = input_tap
            
        for key,val in replacement_values.iteritems():
            flow.source_map[key] = val
        return ret_map
    
    @staticmethod
    def remap_flow_sinks(flow, path = None, mapping={}):
        replacement_values = {}
        ret_map = {}
        for key,val in flow.sink_map.iteritems():
            scheme = val.getScheme()
            orig_out_path = val.path.toString()
            output_path = None
            if orig_out_path in mapping:
                output_path = mapping[orig_out_path]
            elif '*' in mapping:
                output_path = mapping['*']
            else:
                if path is not None:
                    output_path = "%s/%s" %(path, orig_out_path.replace(':','_').replace('/','_'))
            if output_path is not None:
                sink_scheme = MetaScheme.getSinkScheme(scheme, output_path)
                new_tap = cascading.tap.hadoop.Hfs(sink_scheme, output_path, cascading.tap.SinkMode.REPLACE)
                replacement_values[key] = new_tap
                ret_map[orig_out_path] = output_path
            
        for key,val in replacement_values.iteritems():
            flow.sink_map[key] = val
        return ret_map
    

        
    @staticmethod
    def map_run_flow(user_flow, input_str):
        assert(isinstance(user_flow, Flow))
        def gen_flow(input_path, output_path):
            CascadingTestCase.remap_flow_sources(user_flow, mapping = {'*': input_path})
            CascadingTestCase.remap_flow_sinks(user_flow, mapping = {'*' : output_path } )
            return user_flow
            
        return CascadingTestCase.run_flow(gen_flow, input_str)
    
    @staticmethod
    def run_flow(gen_flow, input_str):
        temp_directory = tempfile.mkdtemp()
        try:
            
            #Take the input data received and write it to a temp input file to be read later
            input_filename = "%s/input_file" % (temp_directory)
            f = open(input_filename, "wb")
            f.write(input_str)
            f.close()
            #Generate the output path
            output_path = "%s/out_dir" % (temp_directory)
            flow = gen_flow(input_filename, output_path)
            assert(isinstance(flow, Flow))
            flow.run(num_reducers=1)
            produced_output_str = ""
            for output_file_name in os.listdir(output_path):
                if output_file_name.startswith("part-"):
                    f = open("%s/%s" % (output_path, output_file_name), "rb")
                    produced_output_str += f.read().strip()
                    f.close()
            return produced_output_str
        finally:
            shutil.rmtree(temp_directory)
    
    @staticmethod
    def in_out_run_flow(flow_generator_function, input_str):
        return CascadingTestCase.run_flow(flow_generator_function, input_str)

