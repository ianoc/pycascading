from pycascading.helpers import *
from pycascading.test import *

from feature_test_source import *

class FeatureTestExample(CascadingTestCase):
    def testMapOutput(self):
        flow = get_flow()
        run_results = CascadingTestCase.map_run_flow(flow, "asdfasdf\nsadf asdf asdf\nadsf")
        self.assertEqual(run_results, """adsf\t1
asdf\t2
asdfasdf\t1
sadf\t1""")
    
    def testSimpleOutput(self):
        run_results = CascadingTestCase.in_out_run_flow(get_flow2, "asdfasdf\nsadf asdf asdf\nadsf")
        self.assertEqual(run_results, """adsf\t1
asdf\t2
asdfasdf\t1
sadf\t1""")


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(FeatureTestExample)
    unittest.TextTestRunner(verbosity=2).run(suite)
