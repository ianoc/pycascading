#
# Copyright 2011 Twitter, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Operations related to an Each pipe.

* Add fields to the stream: map_add
* Map fields to new fields: map_replace
* Map the whole tuple to the new tuple: map_to
* Filter tuples: filter_by
* Add fields to the stream using a streaming task: stream_add
"""

__author__ = 'Gabor Szabo'


import inspect

import cascading.pipe
from cascading.tuple import Fields
from com.twitter.pycascading import CascadingStreamFunctionWrapper

import com.twitter.pycascading.expression.ExpressionFilter
import com.twitter.pycascading.expression.ExpressionFunction
from com.twitter.pycascading import CascadingFunctionWrapper, \
CascadingFilterWrapper

from pycascading.pipe import Operation, coerce_to_fields, wrap_function, \
random_pipe_name, DecoratedFunction
from pycascading.decorators import udf


class _Each(Operation):

    """The equivalent of Each in Cascading.

    We need to wrap @maps and @filters with different Java classes, but
    the constructors for Each are built similarly. This class provides this
    functionality.
    """

    def __init__(self, function_type, *args, **kwargs):
        """Build the Each constructor for the Python function.

        Arguments:
        function_type -- CascadingFunctionWrapper or CascadingFilterWrapper,
            whether we are calling Each with a function or filter
        *args -- the arguments passed on to Cascading Each
        **kwargs -- used to pass along the name for this operator
        """
        Operation.__init__(self)

        self._function = None
        # The default argument selector is Fields.ALL (per Cascading sources
        # for Operator.java)
        self._argument_selector = None
        # The default output selector is Fields.RESULTS (per Cascading sources
        # for Operator.java)
        self._output_selector = None

        if len(args) == 1:
            self._function = args[0]
        elif len(args) == 2:
            (self._argument_selector, self._function) = args
        elif len(args) == 3:
            (self._argument_selector, self._function,
             self._output_selector) = args
        else:
            raise Exception('The number of parameters to Apply/Filter ' \
                            'should be between 1 and 3')
        # This is the Cascading Function type
        self._function = wrap_function(self._function, function_type)
        if "name" in kwargs:
            self._name = kwargs["name"]
        else:
            self._name = "each"

    def _create_with_parent(self, parent):
        args = []
        if self._argument_selector:
            args.append(coerce_to_fields(self._argument_selector))
        args.append(self._function)
        if self._output_selector:
            args.append(coerce_to_fields(self._output_selector))
        # We need to put another Pipe after the Each since otherwise
        # joins may not work as the names of pipes apparently have to be
        # different for Cascading.
        each = cascading.pipe.Each(parent.get_assembly(), *args)
        return cascading.pipe.Pipe(random_pipe_name(self._name), each)


class _StringEach(_Each):

    """The equivalent of Each in Cascading.
       
        Seperate from the other Each as we don't have to deal with decorated functions. Though
        since we can't encode the selected output fields into the decorator those must be passed
        through to this function to be useful.
    """

    def __init__(self, function_type, *args):
        """Build the Each constructor for the Python function.

        Arguments:
        function_type -- CascadingStreamFunctionWrapper
        *args -- the arguments passed on to Cascading Each
        """
        Operation.__init__(self)

        self._expression = None
        self._output_selector = None
        self._argument_selector = None
        self._produces = None
        self._name = "ExpressionEach"
        if(len(args) != 4):
            raise Exception('The number of parameters to String each ' \
                        'should be 4')
        (self._produces, self._argument_selector, self._output_selector, self._expression) = \
        (coerce_to_fields(args[0]), coerce_to_fields(args[1]), coerce_to_fields(args[2]), args[3])
        if self._produces is not None:
            self._function = function_type(self._produces, self._expression)
        else:
            self._function = function_type(self._expression)


## three use cases here
# 1) Mutate fields in place
# 2) Add field at the end



class _StreamingEach(_Each):

    """The equivalent of Each in Cascading.
       
        Seperate from the other Each as we don't have to deal with decorated functions. Though
        since we can't encode the selected output fields into the decorator those must be passed
        through to this function to be useful.
    """

    def __init__(self, function_type, *args):
        """Build the Each constructor for the Python function.

        Arguments:
        function_type -- CascadingStreamFunctionWrapper
        *args -- the arguments passed on to Cascading Each
        """
        Operation.__init__(self)
        self._name = "StreamingEach"
        self._function = None
        # The default argument selector is Fields.ALL (per Cascading sources
        # for Operator.java)
        self._argument_selector = None
        # The default output selector is Fields.RESULTS (per Cascading sources
        # for Operator.java)
        self._output_selector = None
        self.__output_record_seperators = None
        
        self.__skipOffset = False

        
        if len(args) != 5:
            raise Exception('The number of parameters to Streaming each ' \
                            'should be 5')
        (self._argument_selector, self._function,
         self._output_selector, self.__output_record_seperators, self.__skipOffset) = args
        

        #Replace the function object with the Java replaced object
        if(self.__skipOffset):
            fw = function_type(coerce_to_fields(["stream_output"]),True)
        else:
            fw = function_type(coerce_to_fields(["stream_offset", "stream_output"]))
            
        fw.setFunction(self._function)
        if self.__output_record_seperators is not None:
            fw.setRecordSeperator(self.__output_record_seperators)

        self._function = fw
    
    
class StreamApply(_StreamingEach):
    """Apply the given user-defined scripting job to each tuple in the stream.

    The corresponding class in Cascading is Each called with a Function.
    """
    def __init__(self, *args):
        _StreamingEach.__init__(self, CascadingStreamFunctionWrapper, *args)
        
class Apply(_Each):
    """Apply the given user-defined function to each tuple in the stream.

    The corresponding class in Cascading is Each called with a Function.
    """
    def __init__(self, *args, **kwargs):
        _Each.__init__(self, CascadingFunctionWrapper, *args, **kwargs)


class Filter(_Each):
    """Filter the tuple stream through the user-defined function.

    The corresponding class in Cascading is Each called with a Filter.
    """
    def __init__(self, *args):
        _Each.__init__(self, CascadingFilterWrapper, *args)


class _ExpressionFilter(_StringEach):
    """Filter the tuple stream through the user-defined function.

    The corresponding class in Cascading is Each called with a Filter.
    """
    def __init__(self, *args):
        _StringEach.__init__(self, com.twitter.pycascading.expression.ExpressionFilter, *args)

        
class _ExpressionApply(_StringEach):
    """Filter the tuple stream through the user-defined function.

    The corresponding class in Cascading is Each called with a Filter.
    """
    def __init__(self, *args):
        _StringEach.__init__(self, com.twitter.pycascading.expression.ExpressionFunction, *args)

def py_expr(*args):
    """ Python expression for mutating fields in the tuples being passed in
        One argument:
            Arg 1) string python expression
            Behavior: Will use the python to mutate existing fields producing the same number of output fields
            
        Two arguments:
            Arg 1) Select from input fields (name or array)
            Arg 2) Expression
            Behavior: Will use the python to mutate the fields selected by the input selector
        
        Three arguments:
            Arg 1) Select from input fields (name or array)
            Arg 2) Expression
            Arg 3) New output field that is the result of the expression
    """
    
    #  (self._produces, self._argument_selector, self.output_selector, self._expression) = (args[0], args[1], args[2], args[3])
        #self._function = function_type(self._produces, expression)
    if len(args) == 1:
        return _ExpressionApply(Fields.ARGS, Fields.ALL, Fields.SWAP, args[0])
    elif len(args) == 2:
        return _ExpressionApply(Fields.ARGS, args[0], Fields.SWAP, args[1])
    elif len(args) == 3:
        return _ExpressionApply(args[2], args[0], Fields.ALL, args[1])
    
def py_filter(expression):
    return _ExpressionFilter(None, Fields.ALL, None, expression)

def _any_instance(var, classes):
    """Check if var is an instance of any class in classes."""
    for cl in classes:
        if isinstance(var, cl):
            return True
    return False


def _map(output_selector, *args, **kwargs):
    """Maps the given input fields to output fields."""
    if len(args) == 1:
        (input_selector, function, output_field) = \
        (Fields.ALL, args[0], Fields.UNKNOWN)
    elif len(args) == 2:
        if inspect.isfunction(args[0]) or _any_instance(args[0], \
        (DecoratedFunction, cascading.operation.Function, cascading.operation.Filter)):
            # The first argument is a function, the second is the output fields
            (input_selector, function, output_field) = \
            (Fields.ALL, args[0], args[1])
        else:
            # The first argument is the input tuple argument selector,
            # the second one is the function
            (input_selector, function, output_field) = \
            (args[0], args[1], Fields.UNKNOWN)
    elif len(args) == 3:
        (input_selector, function, output_field) = args
    else:
        raise Exception('map_{add,replace} needs to be called with 1 to 3 parameters')
    if isinstance(function, DecoratedFunction):
        # By default we take everything from the UDF's decorators
        df = function
        if output_field != Fields.UNKNOWN:
            # But if we specified the output fields for the map, use that
            df = DecoratedFunction.decorate_function(function.decorators['function'])
            df.decorators = dict(function.decorators)
            df.decorators['produces'] = output_field
    elif inspect.isfunction(function):
        df = udf(produces=output_field)(function)
    else:
        df = function
    return Apply(input_selector, df, output_selector, **kwargs)


def map_add(*args, **kwargs):
    """Map the defined fields (or all fields), and add the results to the tuple.

    Note that the new field names we are adding to the tuple cannot overlap
    with existing field names, or Cascading will complain.
    """
    return _map(Fields.ALL, *args, **kwargs)


def map_replace(*args, **kwargs):
    """Map the tuple, remove the mapped fields, and add the new fields.

    This mapping replaces the fields mapped with the new fields that the
    mapping operation adds.

    The number of arguments to this function is between 1 and 3:
    * One argument: it's the map function. The output fields will be named
      after the 'produces' parameter if the map function is decorated, or
      will be Fields.UNKNOWN if it's not defined. Note that after UNKNOW field
      names are introducefd to the tuple, all the other field names are also
      lost.
    * Two arguments: it's either the input field selector and the map function,
      or the map function and the output fields' names.
    * Three arguments: they are interpreted as the input field selector, the
      map function, and finally the output fields' names.
    """
    return _map(Fields.SWAP, *args, **kwargs)


def map_to(*args, **kwargs):
    """Map the tuple, and keep only the results returned by the function."""
    return _map(Fields.RESULTS, *args, **kwargs)


def filter_by(function):
    if isinstance(function, DecoratedFunction):
        # We make sure we will treat the function as a filter
        # Here we make a copy of the decorators so that we don't overwrite
        # the original parameters
        if function.decorators['type'] not in ('filter', 'auto'):
            raise Exception('Function is not a filter')
        df = DecoratedFunction.decorate_function(function.decorators['function'])
        df.decorators = dict(function.decorators)
        df.decorators['type'] = 'filter'
    else:
        df = udf(type='filter')(function)
    return Filter(df)


def _stream_map(output_selector, *args, **kwargs):
    """Maps the given input fields to output fields."""
    # We only ever accept the first tuple
    input_selector = None
    execute_script = None
    output_record_seperators = None
    
    """Maps the given input fields to output fields."""
    if len(args) == 1:
       (input_selector, execute_script) = \
       (Fields.FIRST, args[0])
    elif len(args) == 2:
       (input_selector, execute_script) = args
    else:
       raise Exception('map_{add,replace} needs to be called with 1 to 3 parameters')
    
    
    if "output_record_seperators" in kwargs:
        output_record_seperators = kwargs["output_record_seperators"]
        
    if "skipOffset" in kwargs:
        skipOffset = kwargs["skipOffset"]
    else:
        skipOffset = False
        
    if not isinstance(execute_script, list):
        raise Exception('stream_map_{add,replace} needs to be called where the function is a list')
    return StreamApply(input_selector, execute_script, output_selector, output_record_seperators, skipOffset)

def stream_map_to(*args, **kwargs):
    """Map the tuple, and keep only the results returned by the function."""
    return _stream_map(Fields.RESULTS, *args, **kwargs)

def stream_add(*args, **kwargs):
    """Map the tuple, and add the results returned by the function onto the existing tuple"""
    return _stream_map(Fields.ALL, *args, **kwargs)

def stream_replace(*args, **kwargs):
    """Map the tuple, remove the mapped fields, and add the new fields.

    This mapping replaces the fields mapped with the new fields that the
    mapping operation adds.

    The number of arguments to this function is between 1 and 3:
    * One argument: it's the map function. The output fields will be named
      after the 'produces' parameter if the map function is decorated, or
      will be Fields.UNKNOWN if it's not defined. Note that after UNKNOW field
      names are introducefd to the tuple, all the other field names are also
      lost.
    * Two arguments: it's either the input field selector and the map function,
      or the map function and the output fields' names.
    * Three arguments: they are interpreted as the input field selector, the
      map function, and finally the output fields' names.
    """
    return _stream_map(Fields.SWAP, *args, **kwargs)