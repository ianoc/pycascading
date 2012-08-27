import unittest
import cascading.tuple.Tuple
import os
import tempfile
import shutil
from pycascading.helpers import *
import types

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
            if 'produces' in wrapped_function.decorators:
                return wrapped_function.decorators['produces']
        return None


    def run_udf(self, inputs, function, error_on_different_length_tuple = False):
        def validate_truncate_output(produces, output):
            self.assertTrue(isinstance(output,list))
            if produces is not None:
                if len(produces) != len(output) and error_on_different_length_tuple:
                    raise ProducesOutputMissMatchError("Error the expected length per tuple didn't match the function output")
                output = output[0:len(produces)]
            return output
        assert(isinstance(inputs, list))
        input_tuples = []
        if len(inputs) > 0 and isinstance(inputs[0], list):
            for raw_tuple in inputs:
                input_tuples.append(cascading.tuple.Tuple(raw_tuple))
        else:
            input_tuples.append(cascading.tuple.Tuple(inputs))

        real_fun = self.unwrap_function(function)
        produces = self.get_produces(function)
        outputs = []
        for input_tuple in input_tuples:
            ret = real_fun(input_tuple)
            if isinstance(ret, types.GeneratorType):
                for output in ret:
                    outputs.append(validate_truncate_output(produces, output))
            else:
                outputs.append(validate_truncate_output(produces, ret))
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
    def run_flow(gen_flow, input):
        temp_directory = tempfile.mkdtemp()
        try:
            #Take the input data received and write it to a temp input file to be read later
            input_filename = "%s/input_file" % (temp_directory)
            f = open(input_filename, "wb")
            if isinstance(input, str):
                f.write(input_str)
            elif isinstance(input, list):
                for line in input:
                    first = True
                    for field in line:
                        if first == False:
                            f.write("\t")
                        first = False
                        f.write(str(field))
                    f.write('\n')
                #Presume a list of lists containing all the rows..
            else:
                raise Exception("Unable to understand test input")
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
            if isinstance(input, str):
                return produced_output_str
            else: #It was a list, lets try give back a list
                raw_types = open("%s/.pycascading_types" % (output_path), "rb").read().strip().split("\n")
                types = [s.split("\t")[1].strip() for s in raw_types]
                names = [s.split("\t")[0].strip() for s in raw_types]
                lines = produced_output_str.split("\n")
                output = []
                for line in lines:
                    cur_line = []
                    segments = line.split("\t")
                    if len(segments) != len(types):
                        raise Exception("Unknown error, types and results should match.")
                    for idx in range(len(types)):
                        if types[idx] == "java.lang.String":
                            cur_line.append(segments[idx])
                        elif types[idx] == "java.lang.Double":
                            cur_line.append(float(segments[idx]))
                        elif types[idx] == "java.lang.Float":
                            cur_line.append(float(segments[idx]))
                        elif types[idx] == "java.lang.Integer":
                            cur_line.append(int(segments[idx]))
                        elif types[idx] == "java.lang.Long":
                            cur_line.append(long(segments[idx]))
                        else:
                            raise Exception("Don't know how to handle type : " + str([types[idx]]))
                    output.append(cur_line)
                return (names, output)
        finally:
            shutil.rmtree(temp_directory)
    
    @staticmethod
    def in_out_run_flow(flow_generator_function, input_str):
        return CascadingTestCase.run_flow(flow_generator_function, input_str)

