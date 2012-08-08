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

@udf_map(produces=['word', "extrachar"])
def convert_str_to_int(tuple):
    yield[tuple.get("word"), float(tuple.get("extrachar"))]


def main():
    flow = Flow()
    # The TextLine() scheme produces tuples where the first field is the 
    # offset of the line in the file, and the second is the line as a string.
    input = flow.source(Hfs(TextLine(), 'pycascading_data/town.txt'))
    output = flow.tsv_sink('pycascading_data/out')

    input | stream_map_to("python -u streaming_task.py", ["word", "extrachar"]) | convert_str_to_int | group_by('word', 'extrachar', native.sum("word_count")) | output
    flow.run(num_reducers=1)
