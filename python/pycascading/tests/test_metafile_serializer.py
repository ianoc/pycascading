from pycascading.helpers import *
from pycascading.test import *
from pycascading.py_stream_tools import *
import os

class TestSerializer(CascadingTestCase):
    def testWithStrExp(self):
        def gen_flow1(source, intermediate_path, dest):
            intermediate_file = "%s/intermed1" %(intermediate_path)
            flow1 = Flow()
            input = flow1.source(Hfs(TextDelimited(Fields(['col1', 'col2', 'col3']), '\t',
                                        [String, String, String]), source))
            
            intemediate_out = flow1.tsv_sink(intermediate_file)
            input | intemediate_out
            return flow1
        def gen_flow2(source, intermediate_path, dest):
            intermediate_file = "%s/intermed1" %(intermediate_path)
            flow2 = Flow()
            intermediate_in = flow2.meta_source(intermediate_file)
            output = flow2.tsv_sink(dest)
            intermediate_in | output
            return flow2
        inputs = [["first sec", "Second section", "sadf"],["asdf","asdf", ""],["adsf", "adsf", 'asdf']]
        names,run_results = CascadingTestCase.run_multi_flow([gen_flow1, gen_flow2], inputs)
        self.assertEqual(run_results, inputs)
       
def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSerializer)
    unittest.TextTestRunner(verbosity=2).run(suite)

