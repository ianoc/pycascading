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
"""

from pycascading.helpers import *
from pycascading.py_stream_tools import *

def main():
    flow = Flow()
    # The TextLine() scheme produces tuples where the first field is the 
    # offset of the line in the file, and the second is the line as a string.
    input = flow.source(Hfs(TextLine(), sys.argv[1]))
    output = flow.tsv_sink(sys.argv[2])

    input | py_stream_replace("line", "stream_worker.go", libs = ['nltk'], output_fields=["word", "extrachar"] )  | group_by('word', 'extrachar', native.sum("word_count")) | output
    
    flow.run()
