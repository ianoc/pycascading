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


import os

class TestOperators(CascadingTestCase):
    def testWithoutDiscardDoesntMatch(self):
        def gen_flow(source, dest):

            flow = Flow()
            # The TextLine() scheme produces tuples where the first field is the 
            # offset of the line in the file, and the second is the line as a string.
            input = flow.source(Hfs(TextLine(), source))
            output = flow.tsv_sink(dest)
            input | output
            return flow
        run_results = CascadingTestCase.in_out_run_flow(gen_flow, "asdfasdf\nsadf asdf asdf\nadsf")
        self.assertNotEqual(run_results, "asdfasdf\nsadf asdf asdf\nadsf")
        
    def testWithDiscardMatches(self):
        def gen_flow(source, dest):

            flow = Flow()
            # The TextLine() scheme produces tuples where the first field is the 
            # offset of the line in the file, and the second is the line as a string.
            input = flow.source(Hfs(TextLine(), source))
            output = flow.tsv_sink(dest, meta_sink=False)
            input | discard("offset") | output
            return flow
        run_results = CascadingTestCase.in_out_run_flow(gen_flow, "asdfasdf\nsadf asdf asdf\nadsf")
        self.assertEqual(run_results, "line\nasdfasdf\nsadf asdf asdf\nadsf")
  
    def testWithRename(self):
        def gen_flow(source, dest):

            flow = Flow()
            # The TextLine() scheme produces tuples where the first field is the 
            # offset of the line in the file, and the second is the line as a string.
            input = flow.source(Hfs(TextLine(), source))
            output = flow.tsv_sink(dest, meta_sink=False)
            input | discard("offset") | rename("line", "text") | output
            return flow
        run_results = CascadingTestCase.in_out_run_flow(gen_flow, "asdfasdf\nsadf asdf asdf\nadsf")
        self.assertEqual(run_results, "text\nasdfasdf\nsadf asdf asdf\nadsf")

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestOperators)
    unittest.TextTestRunner(verbosity=2).run(suite)

