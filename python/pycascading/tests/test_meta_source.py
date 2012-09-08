from pycascading.helpers import *
from pycascading.test import *
import os

@udf_map(produces=["mystring", "mylong", "myfloat"])
def make_record(tuple):
    yield ["adsf", 123, 2.2]

@udf_map
def verify_types(tuple):
    string_field = tuple.get("mystring")
    long_field = tuple.get("mylong")
    float_field = tuple.get("myfloat")
    assert(isinstance(long_field, int))
    assert(isinstance(float_field, float))
    assert(isinstance(string_field, unicode))

class TestMetaSource(CascadingTestCase):
    def testWithStrExp(self):
        temp_directory = tempfile.mkdtemp()
        inputstr = "a_field\nfield_val"
        try:
            input_filename = CascadingTestCase._dump_to_path(temp_directory, inputstr)
            #Generate the output path
            output_path = "%s/out_dir" % (temp_directory)
            flow = Flow()
            input = flow.source(Hfs(TextDelimited(True, '\t'), input_filename))
            output = flow.tsv_sink(output_path)
            input | map_to(make_record) | output
            flow.run(num_reducers=1)

            flow2 = Flow()
            input = flow2.tsv_meta_source(output_path)
            output = flow2.tsv_sink("%s/out_dir2" % ( temp_directory ))
            input | verify_types | output
            flow2.run(num_reducers=1)
        finally:
            pass
            #shutil.rmtree(temp_directory)
       
def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMetaSource)
    unittest.TextTestRunner(verbosity=2).run(suite)

