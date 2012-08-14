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

"""Simple word count example."""

from pycascading.helpers import *
from pycascading.test import *




@udf_map(produces=['word'])
def split_words(tuple):
    """The function to split the line and return several new tuples.

    The tuple to operate on is passed in as the first parameter. We are
    yielding the results in a for loop back. Each word becomes the only field
    in a new tuple stream, and the string to be split is the 2nd field of the
    input tuple.
    """
    for word in tuple.get(1).split():
        yield [word]

def get_flow():
    flow = Flow()
    # The TextLine() scheme produces tuples where the first field is the 
    # offset of the line in the file, and the second is the line as a string.
    input = flow.source(Hfs(TextLine(), 'pycascading_data/town.txt'))
    output = flow.tsv_sink('pycascading_data/out')
    input | split_words | group_by('word', native.count()) | output
    return flow

def main():
    flow = get_flow()
    flow.run(num_reducers=2)

def get_flow2(source, dest):
    flow = Flow()
    # The TextLine() scheme produces tuples where the first field is the 
    # offset of the line in the file, and the second is the line as a string.
    input = flow.source(Hfs(TextLine(), source))
    output = flow.tsv_sink(dest)
    input | split_words | group_by('word', native.count()) | output
    return flow
    
def main2():
    flow = get_flow2('pycascading_data/town.txt', 'pycascading_data/out')
    flow.run(num_reducers=2) 
