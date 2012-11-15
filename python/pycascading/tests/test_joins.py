from pycascading.helpers import Flow, Hfs, TextDelimited, Fields, String, inner_join, inner_hash_join, left_outer_hash_join, udf_map, map_add
from pycascading.test import CascadingTestCase, unittest


@udf_map
def add_const(tuple):
    yield [1]


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

    def test_tripleduplicate_field_join(self):
        def gen_flow(sources, dests):
            flow = Flow()
            outputs = [flow.tsv_sink(dests[0])]
            inputs = [
                flow.source(Hfs(TextDelimited(Fields(["a", "b"]), '\t', [String, String]), sources[0])),
                flow.source(Hfs(TextDelimited(Fields(["a", "c"]), '\t', [String, String]), sources[1])),
                flow.source(Hfs(TextDelimited(Fields(["a", "d"]), '\t', [String, String]), sources[2])),
            ]
            inputs[0] & inputs[1] & inputs[2] | inner_join(["a", "a", "a"]) | outputs[0]
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
            [
                [
                    "bob",
                    "yes we can",
                ],  # First row
            ],  # End of third input
        ]  # End of all inputs
        header, results = self.run_flow_with_multiple_in_out(gen_flow, inputs, 1)[0]
        self.assertEqual([['bob', 'builder', 'can we fix it', 'yes we can']], results)

    def test_tripleduplicate_field_hash_join(self):
        def gen_flow(sources, dests):
            flow = Flow()
            outputs = [flow.tsv_sink(dests[0])]
            inputs = [
                flow.source(Hfs(TextDelimited(Fields(["a", "b"]), '\t', [String, String]), sources[0])),
                flow.source(Hfs(TextDelimited(Fields(["a", "c"]), '\t', [String, String]), sources[1])),
                flow.source(Hfs(TextDelimited(Fields(["a", "d"]), '\t', [String, String]), sources[2])),
            ]
            inputs[0] & inputs[1] & inputs[2] | inner_hash_join(["a", "a", "a"]) | outputs[0]
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
            [
                [
                    "bob",
                    "yes we can",
                ],  # First row
            ],  # End of third input
        ]  # End of all inputs
        header, results = self.run_flow_with_multiple_in_out(gen_flow, inputs, 1)[0]
        self.assertEqual([['bob', 'builder', 'can we fix it', 'yes we can']], results)

    def test_normal_hashjoin_join(self):
        def gen_flow(sources, dests):
            flow = Flow()
            outputs = [flow.tsv_sink(dests[0])]
            inputs = [
                flow.source(Hfs(TextDelimited(Fields(["a", "b"]), '\t', [String, String]), sources[0])),
                flow.source(Hfs(TextDelimited(Fields(["c", "d"]), '\t', [String, String]), sources[1])),
            ]
            inputs[0] & inputs[1] | inner_hash_join(["a", "c"]) | outputs[0]
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

    def test_duplicate_field_hash_join(self):
        def gen_flow(sources, dests):
            flow = Flow()
            outputs = [flow.tsv_sink(dests[0])]
            inputs = [
                flow.source(Hfs(TextDelimited(Fields(["a", "b"]), '\t', [String, String]), sources[0])),
                flow.source(Hfs(TextDelimited(Fields(["a", "c"]), '\t', [String, String]), sources[1])),
            ]
            inputs[0] & inputs[1] | inner_hash_join(["a", "a"]) | outputs[0]
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

    def test_left_hash_join(self):
        def gen_flow(sources, dests):
            flow = Flow()
            outputs = [flow.tsv_sink(dests[0])]
            inputs = [
                flow.source(Hfs(TextDelimited(Fields(["a", "b"]), '\t', [String, String]), sources[0])),
                flow.source(Hfs(TextDelimited(Fields(["c", "d"]), '\t', [String, String]), sources[1])),
            ]
            inputs[0] & inputs[1] | left_outer_hash_join(["a", "c"]) | \
                    map_add(add_const, "constant") | outputs[0]
            return flow
        inputs = [
            [
                [
                    "bob",
                    "builder",
                ],  # First row
                [
                    "bob2",
                    "woop",
                ],  # First row
            ],  # End of first input
            [
                [
                    "bob",
                    "can we fix it",
                ],  # First row
                [
                    "bob",
                    "yes we can",
                ],  # Second row
            ],  # End of second input
        ]  # End of all inputs
        header, results = self.run_flow_with_multiple_in_out(gen_flow, inputs, 1)[0]
        self.assertEqual([
            ['bob', 'builder', 'bob', 'can we fix it', 1],
            ['bob', 'builder', 'bob', 'yes we can', 1],
            ['bob2', 'woop', '', '', 1],
            ], results)


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestJoins)
    unittest.TextTestRunner(verbosity=2).run(suite)
