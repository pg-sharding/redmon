#!/usr/bin/env python3
"""
Unit tests for SPQR monitor.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import logging
from spqr_monitor import SPQRMonitor, TaskGroup, KeyRange, setup_logging


class TestSPQRMonitor(unittest.TestCase):
    """Test cases for SPQRMonitor class."""

    def setUp(self):
        """Set up test fixtures."""
        self.logger = logging.getLogger("test")
        self.monitor = SPQRMonitor(
            db_host="localhost",
            db_port=6432,
            db_name="spqr-console",
            db_user="spqr-console",
            dry_run=True,
            logger=self.logger,
        )

    def test_psql_command_generation(self):
        """Test psql command generation."""
        cmd = self.monitor._psql_command("SHOW task_group;")
        self.assertIn("port=6432", cmd)
        self.assertIn("dbname=spqr-console", cmd)
        self.assertIn("user=spqr-console", cmd)
        self.assertIn("SHOW task_group;", cmd)

    def test_execute_command_dry_run(self):
        """Test command execution in dry-run mode."""
        stdout, stderr, code = self.monitor._execute_command("echo test")
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        self.assertEqual(code, 0)

    @patch("subprocess.run")
    def test_execute_command_prod(self, mock_run):
        """Test command execution in production mode."""
        mock_result = Mock()
        mock_result.stdout = "test output"
        mock_result.stderr = ""
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        monitor = SPQRMonitor(
            db_host="localhost",
            db_port=6432,
            db_name="spqr-console",
            db_user="spqr-console",
            dry_run=False,
            logger=self.logger,
        )

        stdout, stderr, code = monitor._execute_command("echo test")
        self.assertEqual(stdout, "test output")
        self.assertEqual(stderr, "")
        self.assertEqual(code, 0)

    def test_parse_task_groups_success(self):
        """Test parsing task groups from psql output."""
        psql_output = """                             task_group_id             | destination_shard_id |                source_key_range_id                 |       destination_key_range_id       |             move_task_id             |  state  |                                             error
--------------------------------------+----------------------+----------------------------------------------------+--------------------------------------+--------------------------------------+---------+------------------------------------------------------------------------------------------------
 07f8dd64-b60a-452d-8c8c-36053b266d60 | shard-005            | ds_user_id_kr_8895479c_634d_4bff_bd0b_6c31f2bd6da5 | f366d2af-1107-42b3-9e68-314aa2cdea0f | fee0b99d-c6af-420d-b415-f25afdf47352 | ERROR   | rpc error: code = Canceled desc = grpc: the client connection is closing
 69cc1380-1a41-44a4-b100-7c340ad450c4 | shard-001            | ds_user_id_kr_19bd3e99_d339_48ff_88de_6072b852d0ee | 00045f2f-aee9-48c3-9248-d0acfac839d3 | cde66b67-0a68-4526-a91e-a202d66a2f00 | ERROR   | rpc error: code = Canceled desc = grpc: the client connection is closing
 5f6d8069-3794-4442-8750-fa0cdfe3b721 | shard-001            | ds_user_id_kr_0584766d_9c2c_438a_8c0c_28678d8954af | b4674c92-1a01-4a8e-8764-b4961a98e352 | 00cf8b2d-6b57-48cd-81b8-76cbc798ad90 | RUNNING |"""

        with patch.object(
            self.monitor, "_execute_command", return_value=(psql_output, "", 0)
        ):
            task_groups = self.monitor.get_task_groups()

        self.assertEqual(len(task_groups), 3)
        self.assertEqual(task_groups[0].task_group_id, "07f8dd64-b60a-452d-8c8c-36053b266d60")
        self.assertEqual(task_groups[0].state, "ERROR")
        self.assertEqual(task_groups[2].state, "RUNNING")

    def test_parse_task_groups_empty(self):
        """Test parsing empty task groups."""
        psql_output = """task_group_id | destination_shard_id | source_key_range_id | destination_key_range_id | move_task_id | state | error
(0 rows)"""

        with patch.object(
            self.monitor, "_execute_command", return_value=(psql_output, "", 0)
        ):
            task_groups = self.monitor.get_task_groups()

        self.assertEqual(len(task_groups), 0)

    def test_parse_key_ranges_success(self):
        """Test parsing key ranges from psql output."""
        psql_output = """                    key_range_id                    | shard_id  | distribution_id |              lower_bound               | locked
----------------------------------------------------+-----------+-----------------+----------------------------------------+--------
 00045f2f-aee9-48c3-9248-d0acfac839d3               | shard-001 | ds_user_id      | '19c6216d-9b6d-4bfb-84ba-2c621110bcd3' | false
 ds_user_id_kr_0                                    | shard0    | ds_user_id      | '00000000-0000-0000-0000-000000000000' | false
 ds_user_id_kr_02efa287_2bc8_4853_aa8f_b0e31d4d1f7d | shard0    | ds_user_id      | '02efa287-2bc8-4853-aa8f-b0e31d4d1f7d' | false"""

        with patch.object(
            self.monitor, "_execute_command", return_value=(psql_output, "", 0)
        ):
            key_ranges = self.monitor.get_key_ranges()

        self.assertEqual(len(key_ranges), 3)
        self.assertEqual(key_ranges[0].shard_id, "shard-001")
        self.assertEqual(key_ranges[1].shard_id, "shard0")
        self.assertEqual(key_ranges[2].shard_id, "shard0")

    def test_find_redistribute_key_range_found(self):
        """Test finding key range to redistribute."""
        key_ranges = [
            KeyRange(
                key_range_id="00045f2f-aee9-48c3-9248-d0acfac839d3",
                shard_id="shard-001",
                lower_bound="'19c6216d-9b6d-4bfb-84ba-2c621110bcd3'",
            ),
            KeyRange(
                key_range_id="ds_user_id_kr_0",
                shard_id="shard0",
                lower_bound="'00000000-0000-0000-0000-000000000000'",
            ),
            KeyRange(
                key_range_id="ds_user_id_kr_02efa287_2bc8_4853_aa8f_b0e31d4d1f7d",
                shard_id="shard0",
                lower_bound="'02efa287-2bc8-4853-aa8f-b0e31d4d1f7d'",
            ),
        ]

        kr = self.monitor.find_redistribute_key_range(key_ranges)
        self.assertIsNotNone(kr)
        self.assertEqual(kr.shard_id, "shard0")
        self.assertTrue(kr.key_range_id.startswith("ds_user_id_kr_"))

    def test_find_redistribute_key_range_not_found(self):
        """Test when no key range to redistribute is found."""
        key_ranges = [
            KeyRange(
                key_range_id="00045f2f-aee9-48c3-9248-d0acfac839d3",
                shard_id="shard-001",
                lower_bound="'19c6216d-9b6d-4bfb-84ba-2c621110bcd3'",
            ),
        ]

        kr = self.monitor.find_redistribute_key_range(key_ranges)
        self.assertIsNone(kr)

    def test_determine_target_shard(self):
        """Test determining target shard based on UUID."""
        # UUID starting with 0 -> shard-001
        target = self.monitor.determine_target_shard("'00000000-0000-0000-0000-000000000000'")
        self.assertEqual(target, "shard-001")
        
        # UUID starting with 2 -> shard-002
        target = self.monitor.determine_target_shard("'2abc1234-5678-90ab-cdef-1234567890ab'")
        self.assertEqual(target, "shard-002")
        
        # UUID starting with e -> shard-008
        target = self.monitor.determine_target_shard("'e0000000-0000-0000-0000-000000000000'")
        self.assertEqual(target, "shard-008")
        
        # UUID starting with f -> shard-008
        target = self.monitor.determine_target_shard("'ffffffff-ffff-ffff-ffff-ffffffffffff'")
        self.assertEqual(target, "shard-008")

    def test_determine_target_shard_invalid_uuid(self):
        """Test when invalid UUID is provided."""
        # Invalid UUID format
        target = self.monitor.determine_target_shard("'invalid-uuid'")
        self.assertIsNone(target)
        
        # No UUID in string
        target = self.monitor.determine_target_shard("'some-bound'")
        self.assertIsNone(target)

    def test_check_read_only_false(self):
        """Test checking read-only when false."""
        with patch.object(
            self.monitor,
            "_execute_command",
            return_value=(" is_read_only \n--------------\n false", "", 0),
        ):
            result = self.monitor.check_read_only()

        self.assertFalse(result)

    def test_check_read_only_true(self):
        """Test checking read-only when true."""
        with patch.object(
            self.monitor,
            "_execute_command",
            return_value=(" is_read_only \n--------------\n true", "", 0),
        ):
            result = self.monitor.check_read_only()

        self.assertTrue(result)

    def test_all_running_enough_true(self):
        """Test all_running_enough when condition is met."""
        task_groups = [
            TaskGroup("tg1", "shard-001", "kr1", "RUNNING"),
            TaskGroup("tg2", "shard-001", "kr2", "RUNNING"),
            TaskGroup("tg3", "shard-001", "kr3", "RUNNING"),
            TaskGroup("tg4", "shard-001", "kr4", "RUNNING"),
            TaskGroup("tg5", "shard-001", "kr5", "RUNNING"),
            TaskGroup("tg6", "shard-001", "kr6", "RUNNING"),
            TaskGroup("tg7", "shard-001", "kr7", "RUNNING"),
            TaskGroup("tg8", "shard-001", "kr8", "RUNNING"),
        ]

        result = self.monitor.all_running_enough(task_groups)
        self.assertTrue(result)

    def test_all_running_enough_false_not_running(self):
        """Test all_running_enough when not all are running."""
        task_groups = [
            TaskGroup("tg1", "shard-001", "kr1", "RUNNING"),
            TaskGroup("tg2", "shard-001", "kr2", "RUNNING"),
            TaskGroup("tg3", "shard-001", "kr3", "ERROR"),
        ]

        result = self.monitor.all_running_enough(task_groups)
        self.assertFalse(result)

    def test_all_running_enough_false_not_enough(self):
        """Test all_running_enough when running count < 8."""
        task_groups = [
            TaskGroup("tg1", "shard-001", "kr1", "RUNNING"),
            TaskGroup("tg2", "shard-001", "kr2", "RUNNING"),
        ]

        result = self.monitor.all_running_enough(task_groups)
        self.assertFalse(result)

    def test_retry_error_task_groups_success(self):
        """Test retrying error task groups."""
        task_groups = [
            TaskGroup("tg1", "shard-001", "kr1", "ERROR", "some error"),
            TaskGroup("tg2", "shard-001", "kr2", "RUNNING"),
        ]

        with patch.object(
            self.monitor, "_execute_command", return_value=("", "", 0)
        ):
            count = self.monitor.retry_error_task_groups(task_groups)

        self.assertEqual(count, 1)

    def test_retry_error_task_groups_max_4(self):
        """Test that max 4 task groups are retried per iteration."""
        task_groups = [
            TaskGroup(f"tg{i}", "shard-001", f"kr{i}", "ERROR")
            for i in range(10)
        ]

        with patch.object(
            self.monitor, "_execute_command", return_value=("", "", 0)
        ):
            count = self.monitor.retry_error_task_groups(task_groups)

        self.assertEqual(count, 4)

    def test_redistribute_key_range_dry_run(self):
        """Test redistribute key range in dry-run mode."""
        result = self.monitor.redistribute_key_range(
            "ds_user_id_kr_test", "shard-001"
        )
        self.assertTrue(result)

    @patch("subprocess.run")
    def test_redistribute_key_range_prod(self, mock_run):
        """Test redistribute key range in production mode."""
        mock_result = Mock()
        mock_result.stdout = ""
        mock_result.stderr = ""
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        monitor = SPQRMonitor(
            db_host="localhost",
            db_port=6432,
            db_name="spqr-console",
            db_user="spqr-console",
            dry_run=False,
            logger=self.logger,
        )

        result = monitor.redistribute_key_range(
            "ds_user_id_kr_test", "shard-001"
        )
        self.assertTrue(result)


class TestLogging(unittest.TestCase):
    """Test logging setup."""

    def test_setup_logging_without_file(self):
        """Test setup logging without file."""
        logger = setup_logging(log_file=None)
        self.assertIsNotNone(logger)

    def test_setup_logging_with_file(self, tmp_path=None):
        """Test setup logging with file."""
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "test.log")
            logger = setup_logging(log_file=log_file)
            self.assertIsNotNone(logger)
            logger.info("Test message")
            self.assertTrue(os.path.exists(log_file))


if __name__ == "__main__":
    unittest.main()
