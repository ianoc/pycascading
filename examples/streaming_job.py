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

"""Simple example and test of using a streaming task.
   One fork of the flow uses a streaming python job to split the lines into words and add 1's to the end of each line.
   The other fork is basically the same achieved entirely within this current process. The diff of the outputs here should be the same.


   * We use sum in the second fork also as if we use count it will produce an integer and then we will not be able to directly diff the outputs of both forks.
"""

from pycascading.helpers import *

@udf_map(produces=['word', "extrachar"])
def convert_str_to_int(tuple):
    yield[tuple.get("word"), int(tuple.get("extrachar"))]

@udf_map(produces=['word', 'extrachar'])
def split_words(tuple):
    """The function to split the line and return several new tuples.

    The tuple to operate on is passed in as the first parameter. We are
    yielding the results in a for loop back. Each word becomes the only field
    in a new tuple stream, and the string to be split is the 2nd field of the
    input tuple.
    """
    for word in tuple.get(1).split():
        yield [word, 1]

@udf_map(produces=['line'])
def get_text(tuple):
    yield [tuple.get(1)]

@udf_map(produces=["word", "extrachar"])
def parse_text(tuple):
    first,_,second = tuple.get(1).partition("\t")
    yield [first, second]


def main():
    flow = Flow()
    # The TextLine() scheme produces tuples where the first field is the 
    # offset of the line in the file, and the second is the line as a string.
    input = flow.source(Hfs(TextLine(), sys.argv[1]))
    output = flow.tsv_sink(sys.argv[2])
    output2 = flow.tsv_sink(sys.argv[3])

    input | get_text | stream_map_to(["python", "streaming_task.py"]) | parse_text | convert_str_to_int | group_by('word', 'extrachar', native.sum("word_count")) | output
    input | split_words | group_by('word', "extrachar", native.sum("word_count")) | output2
    flow.run()
