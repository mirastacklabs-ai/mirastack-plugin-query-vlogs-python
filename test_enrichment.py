"""Tests for query_vlogs_python output enrichment."""

import json
import unittest

from output import MAX_RESULT_LEN, enrich_logs_output


class TestEnrichLogsOutput(unittest.TestCase):
    """Verify output enrichment function."""

    def test_basic_fields(self):
        out = enrich_logs_output("stats", '{"hits":42}')
        self.assertEqual(out["action"], "stats")

    def test_ndjson_counting(self):
        ndjson = '{"msg":"a"}\n{"msg":"b"}\n{"msg":"c"}\n'
        out = enrich_logs_output("query", ndjson)
        self.assertEqual(out["result_count"], 3)

    def test_tail_ndjson_counting(self):
        ndjson = '{"msg":"x"}\n{"msg":"y"}\n'
        out = enrich_logs_output("tail", ndjson)
        self.assertEqual(out["result_count"], 2)

    def test_json_array_counting(self):
        raw = json.dumps(["field1", "field2", "field3"])
        out = enrich_logs_output("field_names", raw)
        self.assertEqual(out["result_count"], 3)

    def test_values_key_counting(self):
        raw = json.dumps({"values": ["v1", "v2"]})
        out = enrich_logs_output("field_values", raw)
        self.assertEqual(out["result_count"], 2)

    def test_truncation(self):
        long = "x" * (MAX_RESULT_LEN + 5000)
        out = enrich_logs_output("query", long)
        self.assertTrue(out["truncated"])
        self.assertEqual(len(out["result"]), MAX_RESULT_LEN)

    def test_invalid_json_passes_through(self):
        out = enrich_logs_output("stats", "not-json")
        self.assertEqual(out["action"], "stats")
        self.assertEqual(out["result"], "not-json")


if __name__ == "__main__":
    unittest.main()
