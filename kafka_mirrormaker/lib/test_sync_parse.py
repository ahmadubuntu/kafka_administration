#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sync_parse import (
    acl_argv_lists,
    acl_cli_args,
    acl_diffs,
    alters_by_topic,
    config_diffs,
    format_add_config,
    parse_acls,
    parse_topic_configs_all,
)


class TestConfigs(unittest.TestCase):
    SAMPLE = """
All configs for topic transactions are:
  cleanup.policy=delete sensitive=false synonyms={DEFAULT_CONFIG:log.cleanup.policy=delete}
  compression.type=producer sensitive=false synonyms={DEFAULT_CONFIG:compression.type=producer}
  message.timestamp.type=LogAppendTime sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:message.timestamp.type=LogAppendTime}
  retention.ms=259200000 sensitive=false
  follower.replication.throttled.replicas= sensitive=false synonyms={}
All configs for topic events are:
  retention.ms=86400000 sensitive=false
"""

    DEST = """
All configs for topic transactions are:
  cleanup.policy=delete sensitive=false
  compression.type=producer sensitive=false
  message.timestamp.type=CreateTime sensitive=false
  retention.ms=259200000 sensitive=false
All configs for topic events are:
  retention.ms=86400000 sensitive=false
"""

    def test_parse_and_diff(self) -> None:
        src = parse_topic_configs_all(self.SAMPLE)
        dst = parse_topic_configs_all(self.DEST)
        self.assertEqual(src["transactions"]["message.timestamp.type"], "LogAppendTime")
        diffs = config_diffs(src, dst)
        keys = {(d["topic"], d["key"]) for d in diffs}
        self.assertIn(("transactions", "message.timestamp.type"), keys)
        self.assertNotIn(("transactions", "follower.replication.throttled.replicas"), keys)
        al = alters_by_topic(diffs)
        self.assertEqual(al["transactions"], [("message.timestamp.type", "LogAppendTime")])

    def test_missing_dest_topic(self) -> None:
        src = {"onlysrc": {"retention.ms": "1"}}
        diffs = config_diffs(src, {})
        self.assertEqual(diffs[0]["dst"], "MISSING_TOPIC")
        self.assertEqual(diffs[0]["action"], "skip")
        self.assertEqual(alters_by_topic(diffs), {})

    def test_add_config_split(self) -> None:
        chunks = format_add_config([("a", "1"), ("b", "x,y"), ("c", "3")])
        self.assertEqual(chunks, ["a=1", "b=x,y", "c=3"])


class TestAcls(unittest.TestCase):
    SRC = """
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=transactions, patternType=LITERAL)`:
 	(principal=User:app, host=*, operation=Read, permissionType=ALLOW)
 	(principal=User:app, host=*, operation=Write, permissionType=ALLOW)
Current ACLs for resource `ResourcePattern(resourceType=GROUP, name=cg-foo, patternType=LITERAL)`:
 	(principal=User:app, host=*, operation=Read, permissionType=ALLOW)
"""
    DST = """
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=transactions, patternType=LITERAL)`:
 	(principal=User:app, host=*, operation=Read, permissionType=ALLOW)
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=extra-dr, patternType=LITERAL)`:
 	(principal=User:other, host=*, operation=Describe, permissionType=ALLOW)
"""

    def test_add_and_extra(self) -> None:
        d = acl_diffs(parse_acls(self.SRC), parse_acls(self.DST))
        add_ops = {(x["name"], x["operation"]) for x in d["add"]}
        self.assertIn(("transactions", "WRITE"), add_ops)
        self.assertIn(("cg-foo", "READ"), add_ops)
        extra_names = {x["name"] for x in d["extra"]}
        self.assertIn("extra-dr", extra_names)
        argv = acl_cli_args(d["add"][0])
        self.assertIn("--add", argv)
        self.assertIn("--allow-principal", argv)

    def test_user_resource_argv(self) -> None:
        src = """
Current ACLs for resource `ResourcePattern(resourceType=USER, name=User:app, patternType=LITERAL)`:
 	(principal=User:admin, host=*, operation=Describe, permissionType=ALLOW)
"""
        dst = """
Current ACLs for resource `ResourcePattern(resourceType=USER, name=User:extra, patternType=LITERAL)`:
 	(principal=User:other, host=*, operation=Describe, permissionType=ALLOW)
"""
        d = acl_diffs(parse_acls(src), parse_acls(dst))
        add_argv, skip_add = acl_argv_lists(d["add"])
        rm_argv, skip_rm = acl_argv_lists(d["extra"], remove=True)
        self.assertEqual(skip_add, [])
        self.assertEqual(skip_rm, [])
        self.assertIn("--user-principal", add_argv[0])
        self.assertIn("User:app", add_argv[0])
        self.assertIn("--user-principal", rm_argv[0])
        self.assertIn("User:extra", rm_argv[0])

    def test_unknown_resource_skipped(self) -> None:
        rows = [
            {
                "rtype": "NOTATYPE",
                "name": "x",
                "pattern": "LITERAL",
                "principal": "User:a",
                "host": "*",
                "operation": "READ",
                "perm": "ALLOW",
            }
        ]
        argv, skipped = acl_argv_lists(rows)
        self.assertEqual(argv, [])
        self.assertEqual(len(skipped), 1)
        self.assertIn("NOTATYPE", skipped[0]["reason"])


if __name__ == "__main__":
    unittest.main()
