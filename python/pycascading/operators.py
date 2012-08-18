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

"""Various operations acting on the tuples.

* Select fields from the stream: retain
* Remove fields from the stream: discard (not implemented in Cascading 1.2.*)
* Rename fields: rename
"""

__author__ = 'Gabor Szabo'


import itertools

from cascading.tuple import Fields
from cascading.operation import Identity
import cascading.pipe.assembly.Rename
import cascading.pipe.assembly.Discard
import cascading.operation.expression.ExpressionFilter
from pycascading.pipe import SubAssembly, coerce_to_fields
from pycascading.each import Apply


def retain(*fields_to_keep):
    """Retain only the given fields.

    The fields can be given in array or by separate parameters.
    """
    if len(fields_to_keep) > 1:
        fields_to_keep = list(itertools.chain(fields_to_keep))
    else:
        fields_to_keep = fields_to_keep[0]
    return Apply(fields_to_keep, Identity(Fields.ARGS), Fields.RESULTS)


def discard(fields_to_discard):
    return SubAssembly(cascading.pipe.assembly.Discard,\
                       coerce_to_fields(fields_to_discard))
 
def expression_filter(*args):
    fields_for_expression, expression_str, types = (None, None, None)
    if len(args) == 2:
        expression_str = args[0]
        types = args[1]
    elif len(args) == 3:
        fields_for_expression = args[0]
        expression_str = args[1]
        types = args[2]
        if not isinstance(types, list):
            types = [types]

    if fields_for_expression is not None:
        if isinstance(fields_for_expression, str):
            fields_for_expression = [fields_for_expression]
    if fields_for_expression is not None:
        return cascading.operation.expression.ExpressionFilter(expression_str, fields_for_expression, types)
    else:
        return cascading.operation.expression.ExpressionFilter(expression_str, types)

import java.lang.Object
baseClass = java.lang.Object()

def l_exp_filter(expression):
    import java.lang.Long
    return expression_filter(expression, java.lang.Long.TYPE)

def str_exp_filter(expression):
    baseClass = java.lang.String()
    return expression_filter(expression, baseClass.class)

def f_exp_filter(expression):
    import java.lang.Float
    return expression_filter(expression, java.lang.Float.TYPE)

def rename(*args):
    """Rename the fields to new names.

    If only one argument (a list of names) is given, it is assumed that the
    user wants to rename all the fields. If there are two arguments, the first
    list is the set of fields to be renamed, and the second is a list of the
    new names.
    """
    if len(args) == 1:
        (fields_from, fields_to) = (Fields.ALL, args[0])
    else:
        (fields_from, fields_to) = (args[0], args[1])
    return SubAssembly(cascading.pipe.assembly.Rename, \
                       coerce_to_fields(fields_from), \
                       coerce_to_fields(fields_to))
