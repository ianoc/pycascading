import unittest
import cascading.tuple.Tuple
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


    def get_outputs_for_inputs(self, inputs, function, error_on_different_length_tuple = False):
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
