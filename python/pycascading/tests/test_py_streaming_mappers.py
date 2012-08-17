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
from pycascading.py_stream_tools import *



import os

class TestPyStreamingMappers(CascadingTestCase):
    def setUp(self):
        cwd = os.path.abspath(os.path.dirname(__file__))
        self.__old_dir = os.getcwd()
        os.chdir(cwd)
    def tearDown(self):
        if self.__old_dir is not None:
            os.chdir(self.__old_dir)

    def testMapTo(self):
        def gen_flow(source, dest):

            flow = Flow()
            # The TextLine() scheme produces tuples where the first field is the 
            # offset of the line in the file, and the second is the line as a string.
            input = flow.source(Hfs(TextLine(), source))
            output = flow.tsv_sink(dest)
            input | py_stream_to("line", PyMod("py_streaming_mapper_child_1.go"), ["word", "count"]) | group_by('word', native.count()) | output
            return flow
        run_results = CascadingTestCase.in_out_run_flow(gen_flow, "asdfasdf\nsadf asdf asdf\nadsf")
        self.assertEqual(run_results, """adsf\t1
asdf\t2
asdfasdf\t1
sadf\t1""")
    
    def testAdd(self):
        def gen_streaming_flow(source, dest):
            flow = Flow()
            # The TextLine() scheme produces tuples where the first field is the 
            # offset of the line in the file, and the second is the line as a string.
            input = flow.source(Hfs(TextLine(), source))
            output = flow.tsv_sink(dest)

            input |  py_stream_add("line", PyMod("py_streaming_mapper_child_1.go"), ["word", "count"]) | output
            return flow
        
        def gen_alternate_flow(source,dest):
            @udf_map(produces=["word", "count"])
            def fake_streamer(tuple):
                for x in tuple.get(0).split(" "):
                    yield [x, 1]
            flow = Flow()
            # The TextLine() scheme produces tuples where the first field is the 
            # offset of the line in the file, and the second is the line as a string.
            input = flow.source(Hfs(TextLine(), source))
            output = flow.tsv_sink(dest)
            input | map_add("line", fake_streamer)  | output
            return flow
        
        streaming_results = CascadingTestCase.in_out_run_flow(gen_streaming_flow, "my line one\nmy second line\nmy third line")
        alternate_results = CascadingTestCase.in_out_run_flow(gen_alternate_flow, "my line one\nmy second line\nmy third line")
        self.assertEqual(streaming_results, alternate_results)
    
    def testReplace(self):
        def gen_streaming_flow(source, dest):
            flow = Flow()
            # The TextLine() scheme produces tuples where the first field is the 
            # offset of the line in the file, and the second is the line as a string.
            input = flow.source(Hfs(TextLine(), source))
            output = flow.tsv_sink(dest)
            input |  py_stream_replace("line", PyMod("py_streaming_mapper_child_1.go"), ["word", "count"]) | output
            return flow
        
        def gen_alternate_flow(source,dest):
            @udf_map(produces=["word", "count"])
            def fake_streamer(tuple):
                for x in tuple.get(0).split(" "):
                    yield [x, 1]
            flow = Flow()
            # The TextLine() scheme produces tuples where the first field is the 
            # offset of the line in the file, and the second is the line as a string.
            input = flow.source(Hfs(TextLine(), source))
            output = flow.tsv_sink(dest)
            input | map_replace("line", fake_streamer) | output
            return flow
        
        streaming_results = CascadingTestCase.in_out_run_flow(gen_streaming_flow, "my line one\nmy second line\nmy third line")
        alternate_results = CascadingTestCase.in_out_run_flow(gen_alternate_flow, "my line one\nmy second line\nmy third line")
        self.assertEqual(streaming_results, alternate_results)
        
    def testMapTo(self):
        def gen_streaming_flow(source, dest):
            flow = Flow()
            # The TextLine() scheme produces tuples where the first field is the 
            # offset of the line in the file, and the second is the line as a string.
            input = flow.source(Hfs(TextLine(), source))
            output = flow.tsv_sink(dest)
            input |  py_stream_to("line", PyMod("py_streaming_mapper_child_1.go"), ["word", "count"]) | output
            return flow
        
        def gen_alternate_flow(source,dest):
            @udf_map(produces=["word", "count"])
            def fake_streamer(tuple):
                for x in tuple.get(0).split(" "):
                    yield [x, 1]
            flow = Flow()
            # The TextLine() scheme produces tuples where the first field is the 
            # offset of the line in the file, and the second is the line as a string.
            input = flow.source(Hfs(TextLine(), source))
            output = flow.tsv_sink(dest)
            input | retain("line") | map_to(fake_streamer) | output
            return flow
        
        streaming_results = CascadingTestCase.in_out_run_flow(gen_streaming_flow, "my line one\nmy second line\nmy third line")
        alternate_results = CascadingTestCase.in_out_run_flow(gen_alternate_flow, "my line one\nmy second line\nmy third line")
        self.assertEqual(streaming_results, alternate_results)

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPyStreamingMappers)
    unittest.TextTestRunner(verbosity=2).run(suite)

