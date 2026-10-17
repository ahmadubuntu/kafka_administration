#!/usr/bin/env python3
"""Unit tests for benchmark_parse.py."""

import unittest

from benchmark_parse import (
    parse_consumer_perf,
    parse_producer_perf,
    summarize_timings_ms,
)


class TestBenchmarkParse(unittest.TestCase):
    def test_parse_producer_perf_multi_line(self) -> None:
        sample = """
19998 records sent, 3999.6 records/sec (3.91 MB/sec), 1.8 ms avg latency, 142.0 ms max latency.
50000 records sent, 12450.20 records/sec (12.16 MB/sec), 2.35 ms avg latency, 185.00 ms max latency, 1.00 ms 50th, 6.00 ms 95th, 14.00 ms 99th, 48.00 ms 99.9th.
"""
        res = parse_producer_perf(sample)
        self.assertTrue(res["ok"])
        self.assertEqual(res["records_sent"], 50000)
        self.assertAlmostEqual(res["records_per_sec"], 12450.20)
        self.assertAlmostEqual(res["mb_per_sec"], 12.16)
        self.assertAlmostEqual(res["avg_latency_ms"], 2.35)
        self.assertAlmostEqual(res["max_latency_ms"], 185.00)
        self.assertAlmostEqual(res["p50_latency_ms"], 1.00)
        self.assertAlmostEqual(res["p95_latency_ms"], 6.00)
        self.assertAlmostEqual(res["p99_latency_ms"], 14.00)
        self.assertAlmostEqual(res["p999_latency_ms"], 48.00)

    def test_parse_consumer_perf_standard_csv(self) -> None:
        sample = """
SLF4J: Class path contains multiple SLF4J bindings.
start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
2026-10-17 07:10:01:123, 2026-10-17 07:10:03:456, 48.8281, 20.9293, 50000, 21431.6331, 312, 2021, 24.1604, 24740.2276
"""
        res = parse_consumer_perf(sample)
        self.assertTrue(res["ok"])
        self.assertAlmostEqual(res["data_consumed_mb"], 48.8281)
        self.assertAlmostEqual(res["mb_per_sec"], 20.9293)
        self.assertEqual(res["records_consumed"], 50000)
        self.assertAlmostEqual(res["records_per_sec"], 21431.6331)
        self.assertEqual(res["rebalance_time_ms"], 312)
        self.assertEqual(res["fetch_time_ms"], 2021)
        self.assertAlmostEqual(res["fetch_mb_per_sec"], 24.1604)
        self.assertAlmostEqual(res["fetch_records_per_sec"], 24740.2276)

    def test_summarize_timings_ms(self) -> None:
        stats = summarize_timings_ms([1850.4, 1420.2, 1389.4])
        self.assertEqual(stats["count"], 3)
        self.assertAlmostEqual(stats["cold_ms"], 1850.4)
        self.assertAlmostEqual(stats["min_ms"], 1389.4)
        self.assertAlmostEqual(stats["max_ms"], 1850.4)
        self.assertAlmostEqual(stats["avg_ms"], 1553.33)


if __name__ == "__main__":
    unittest.main()
