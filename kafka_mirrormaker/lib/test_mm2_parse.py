#!/usr/bin/env python3
"""Offline parse checks for log-dirs JSON/text and topic configs."""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mm2_parse import (
    parse_logdirs,
    parse_topic_configs,
    is_mm2_internal,
    recommend_dest_compression,
)


class TestLogdirs(unittest.TestCase):
    def test_json_dict_partitions(self) -> None:
        text = """
Querying brokers for log directories information
Received log directory information from brokers 1
{"version":1,"brokers":[{"broker":1,"logDirs":[{"logDir":"/data","error":null,"partitions":{"orders-0":{"size":1073741824,"offsetLag":0,"isFuture":false}}}]}]}
"""
        rows = parse_logdirs(text)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["topic"], "orders")
        self.assertEqual(rows[0]["partition"], 0)
        self.assertEqual(rows[0]["size"], 1073741824)
        self.assertEqual(str(rows[0]["broker"]), "1")

    def test_json_array_partitions(self) -> None:
        text = """
{"version":2,"brokers":[{"broker":2,"logDirs":[{"logDir":"/kafka","error":null,"partitions":[{"partition":"payments-3","size":42,"offsetLag":0,"isFuture":false}]}]}]}
"""
        rows = parse_logdirs(text)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["topic"], "payments")
        self.assertEqual(rows[0]["partition"], 3)
        self.assertEqual(rows[0]["size"], 42)

    def test_text_topic_partition_size(self) -> None:
        text = """
Broker: 7
log directory: /var/opt/kafka/logs
topic: foo.bar partition: 1 size: 99
"""
        rows = parse_logdirs(text)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["topic"], "foo.bar")
        self.assertEqual(rows[0]["partition"], 1)
        self.assertEqual(rows[0]["size"], 99)
        self.assertEqual(str(rows[0]["broker"]), "7")

    def test_text_tp_compact(self) -> None:
        text = "broker 3\norders-0: size=12345\n"
        rows = parse_logdirs(text)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["topic"], "orders")
        self.assertEqual(rows[0]["size"], 12345)


class TestConfigs(unittest.TestCase):
    def test_inline_and_indented(self) -> None:
        text = """
Configs for topic foo are: retention.ms=86400000,cleanup.policy=delete
Dynamic configs for topic bar are:
  retention.bytes=1000 sensitive=false
"""
        cfg = parse_topic_configs(text)
        self.assertEqual(cfg["foo"]["retention.ms"], "86400000")
        self.assertEqual(cfg["foo"]["cleanup.policy"], "delete")
        self.assertEqual(cfg["bar"]["retention.bytes"], "1000")

    def test_all_configs_compression(self) -> None:
        text = """
All configs for topic transactions are:
  compression.type=producer sensitive=false synonyms={DEFAULT_CONFIG:compression.type=producer}
  retention.ms=259200000 sensitive=false
"""
        cfg = parse_topic_configs(text)
        self.assertEqual(cfg["transactions"]["compression.type"], "producer")
        self.assertEqual(cfg["transactions"]["retention.ms"], "259200000")


class TestDestCompression(unittest.TestCase):
    def test_explicit_src_codec_only(self) -> None:
        extras = [(10**9, "orders", "orders", 100, 200)]
        recs = recommend_dest_compression(
            extras,
            {"orders": {"compression.type": "zstd"}},
            {"orders": {"compression.type": "producer"}},
            "asia-gen",
            "afra-gen",
        )
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["codec"], "zstd")
        self.assertEqual(recs[0]["reason"], "src_topic_config")

    def test_no_global_on_small_ratio(self) -> None:
        extras = [(10**8, "tiny", "tiny", 10**9, int(1.1 * 10**9))]
        recs = recommend_dest_compression(
            extras,
            {"tiny": {"compression.type": "producer"}},
            {"tiny": {"compression.type": "producer"}},
            "asia-gen",
            "afra-gen",
        )
        self.assertEqual(recs, [])

    def test_size_ratio_inferred(self) -> None:
        extras = [(134 * 10**9, "transactions", "transactions", 51 * 10**9, 186 * 10**9)]
        recs = recommend_dest_compression(
            extras,
            {"transactions": {"compression.type": "producer"}},
            {"transactions": {"compression.type": "producer"}},
            "asia-gen",
            "afra-gen",
        )
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["codec"], "lz4")
        self.assertEqual(recs[0]["reason"], "size_ratio_implies_src_compressed")


class TestInternal(unittest.TestCase):
    def test_heartbeats_not_nested(self) -> None:
        self.assertEqual(is_mm2_internal("asia-gen.heartbeats", "asia-gen", "afra-gen"), "heartbeats")
        self.assertEqual(
            is_mm2_internal("mm2-offset-syncs.asia-gen.internal", "asia-gen", "afra-gen"),
            "offset-syncs",
        )


if __name__ == "__main__":
    unittest.main()
