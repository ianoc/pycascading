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


cwd = os.path.abspath(os.path.dirname(__file__))

class TestStreamingMappers(CascadingTestCase):
    def testMapTo(self):
        def gen_flow(source, dest):
            
            @udf_map(produces=["word", "count"])
            def deserialize_string(tuple):
                f1, f2 = tuple.get(1).split("\t")
                yield [f1,f2]

            flow = Flow()
            # The TextLine() scheme produces tuples where the first field is the 
            # offset of the line in the file, and the second is the line as a string.
            input = flow.source(Hfs(TextLine(), source))
            output = flow.tsv_sink(dest)
            input | stream_map_to("line",["python", "-u", os.path.join(cwd,  "streaming_mapper_child_1.py")]) | deserialize_string | group_by('word', native.count()) | output
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
            #input | map_add("line", fake_streamer) | output
            input | stream_add("line", ["python", "-u", os.path.join(cwd, "streaming_mapper_child_1.py")], skipOffset=True) | output
            return flow
        
        def gen_alternate_flow(source,dest):
            @udf_map(produces=["stream_output"])
            def fake_streamer(tuple):
                for x in tuple.get(0).split(" "):
                    yield ["%s\t1" % (x)]
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
            #input | map_add("line", fake_streamer) | output
            input | stream_replace("line", ["python", "-u", os.path.join(cwd, "streaming_mapper_child_1.py")], skipOffset=True) | output
            return flow
        
        def gen_alternate_flow(source,dest):
            @udf_map(produces=["stream_output"])
            def fake_streamer(tuple):
                for x in tuple.get(0).split(" "):
                    yield ["%s\t1" % (x)]
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
            #input | map_add("line", fake_streamer) | output
            input | retain("line") | stream_map_to(["python", "-u", os.path.join(cwd, "streaming_mapper_child_1.py")], skipOffset=True) | output
            return flow
        
        def gen_alternate_flow(source,dest):
            @udf_map(produces=["stream_output"])
            def fake_streamer(tuple):
                for x in tuple.get(0).split(" "):
                    yield ["%s\t1" % (x)]
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
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStreamingMappers)
    unittest.TextTestRunner(verbosity=2).run(suite)

