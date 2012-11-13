from pycascading.helpers import Flow, Hfs, TextDelimited, Fields, String, inner_join
from pycascading.test import CascadingTestCase, unittest


class TestJoins(CascadingTestCase):
    def test_normal_join(self):
        def gen_flow(sources, dests):
            flow = Flow()
            outputs = [flow.tsv_sink(dests[0])]
            inputs = [
                flow.source(Hfs(TextDelimited(Fields(["a", "b"]), '\t', [String, String]), sources[0])),
                flow.source(Hfs(TextDelimited(Fields(["c", "d"]), '\t', [String, String]), sources[1])),
            ]
            inputs[0] & inputs[1] | inner_join(["a", "c"]) | outputs[0]
            return flow
        inputs = [
            [
                [
                    "bob",
                    "builder",
                ],  # First row
            ],  # End of first input
            [
                [
                    "bob",
                    "can we fix it",
                ],  # First row
            ],  # End of second input
        ]  # End of all inputs
        header, results = self.run_flow_with_multiple_in_out(gen_flow, inputs, 1)[0]
        self.assertEqual([['bob', 'builder', 'bob', 'can we fix it']], results)

    def test_duplicate_field_join(self):
        def gen_flow(sources, dests):
            flow = Flow()
            outputs = [flow.tsv_sink(dests[0])]
            inputs = [
                flow.source(Hfs(TextDelimited(Fields(["a", "b"]), '\t', [String, String]), sources[0])),
                flow.source(Hfs(TextDelimited(Fields(["a", "c"]), '\t', [String, String]), sources[1])),
            ]
            inputs[0] & inputs[1] | inner_join(["a", "a"]) | outputs[0]
            return flow
        inputs = [
            [
                [
                    "bob",
                    "builder",
                ],  # First row
            ],  # End of first input
            [
                [
                    "bob",
                    "can we fix it",
                ],  # First row
            ],  # End of second input
        ]  # End of all inputs
        header, results = self.run_flow_with_multiple_in_out(gen_flow, inputs, 1)[0]
        self.assertEqual([['bob', 'builder', 'can we fix it']], results)

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestJoins)
    unittest.TextTestRunner(verbosity=2).run(suite)
