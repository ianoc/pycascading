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
        header, run_results = CascadingTestCase.in_out_run_flow(gen_flow, [
                                            ["asdfasdf"],
                                            ["sadf asdf asdf"],
                                            ["adsf"]
                                        ])
        self.assertNotEqual(run_results, [["asdfasdf"], ["sadf asdf asdf"], ["adsf"]])

    def testWithDiscardMatches(self):
        def gen_flow(source, dest):

            flow = Flow()
            # The TextLine() scheme produces tuples where the first field is the
            # offset of the line in the file, and the second is the line as a string.
            input = flow.source(Hfs(TextLine(), source))
            output = flow.tsv_sink(dest)
            input | discard("offset") | output
            return flow
        header, run_results = CascadingTestCase.in_out_run_flow(gen_flow, [
                                            ["asdfasdf"],
                                            ["sadf asdf asdf"],
                                            ["adsf"]
                                        ])
        self.assertEqual(run_results, [["asdfasdf"], ["sadf asdf asdf"], ["adsf"]])

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

    def testWithLongExp(self):
        def gen_flow(source, dest):

            flow = Flow()
            input = flow.source(Hfs(TextDelimited(Fields(['col1', 'col2']), '\t',
                                        [Integer, Integer]), source))

            output = flow.tsv_sink(dest, meta_sink=False)

            input | l_exp_filter("col1 > 10") | output
            return flow
        run_results = CascadingTestCase.in_out_run_flow(gen_flow, "3\t5\n5\t8\n11\t3\n50\t1")
        self.assertEqual(run_results, "col1\tcol2\n3\t5\n5\t8")

    def testWithFloatExp(self):

        def gen_flow(source, dest):
            flow = Flow()
            input = flow.source(Hfs(TextDelimited(Fields(['col1', 'col2']), '\t',
                                        [Float, Float]), source))
            output = flow.tsv_sink(dest, meta_sink=False)
            input | f_exp_filter("col1 > 2.5") | output
            return flow

        run_results = CascadingTestCase.in_out_run_flow(gen_flow, "3.0\t5\n5\t8\n3.1\t3\n2.0\t1")
        self.assertEqual(run_results, "col1\tcol2\n2.0\t1.0")

    def testWithStrExp(self):
        def gen_flow(source, dest):
           flow = Flow()
           input = flow.source(Hfs(TextDelimited(Fields(['col1', 'col2']), '\t',
                                       [String, String]), source))
           output = flow.tsv_sink(dest, meta_sink=False)

           input | str_exp_filter("col1.length() > 4") | output
           return flow
        run_results = CascadingTestCase.in_out_run_flow(gen_flow, "first sec\tSecond sectoin\nsadf \tasdf asdf\nadsf\tadsf")
        self.assertEqual(run_results, "col1\tcol2\nadsf\tadsf")

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestOperators)
    unittest.TextTestRunner(verbosity=2).run(suite)
