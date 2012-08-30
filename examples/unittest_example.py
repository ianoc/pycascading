from pycascading.test import *
import cascading.tuple.Tuple
from pycascading.helpers import *

import re

@udf_map(produces=['doc_id', 'token'])
def wrapped_case(tuple):
    for word in re.split("[ \\[\\]\\(\\),.]", tuple.get(1)):
        if len(word) > 0:
            yield [tuple.get(0), word]


@udf_map(produces=['doc_id', 'token'])
def wrapped_case_extra_arg(tuple, gt_len):
    for word in re.split("[ \\[\\]\\(\\),.]", tuple.get(1)):
        if len(word) > gt_len:
            yield [tuple.get(0), word]

@udf_map(produces=['doc_id', 'token'])
def wrapped_case_keyword_extra_arg(tuple, gt_len = 0):
    for word in re.split("[ \\[\\]\\(\\),.]", tuple.get(1)):
        if len(word) > gt_len:
            yield [tuple.get(0), word]

def unwrapped_case(tuple):
    for word in re.split("[ \\[\\]\\(\\),.]", tuple.get(1)):
        if len(word) > 0:
            yield [tuple.get(0), word]

@udf_map(produces=['doc_id'])
def produce_not_match_output(tuple):
    for word in re.split("[ \\[\\]\\(\\),.]", tuple.get(1)):
        if len(word) > 0:
            yield [tuple.get(0), word]


class UnitTestExample(CascadingTestCase):
    def testWrapped(self):
        results = self.run_udf([33,'arg1 arg2 arg3'], wrapped_case)
        self.assertEqual(3, len(results))

    def wrapped_case_extra_arg(self):
        results = self.run_udf([33,'arg1 arg2 arg3 longerArg'], wrapped_case_extra_arg(3))
        self.assertEqual(1, len(results))

    def wrapped_case_keyword_extra_arg(self):
        results = self.run_udf([33,'arg1 arg2 arg3 longerArg'], wrapped_case_keyword_extra_arg(gt_len = 3))
        self.assertEqual(1, len(results))

    def testUnWrapped(self):
        results = self.run_udf([33,'arg1 arg2 arg3'], unwrapped_case)
        self.assertEqual(3, len(results))

    def testProducesMatchesOutput(self):
        results = self.run_udf([33,'arg1 arg2 arg3'], wrapped_case, error_on_different_length_tuple = True)
        self.assertEqual(3, len(results))

    def testProducesFailsToMatchOutput(self):
        self.assertRaises(ProducesOutputMissMatchError, self.run_udf, [33,'arg1 arg2 arg3'], produce_not_match_output, error_on_different_length_tuple = True)

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(UnitTestExample)
    unittest.TextTestRunner(verbosity=2).run(suite)
