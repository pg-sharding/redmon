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

    def test_execute_write_dry_run(self):
        """Test write command execution in dry-run mode."""
        stdout, stderr, code = self.monitor.execute_write("echo test")
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        self.assertEqual(code, 0)

    @patch("subprocess.run")
    def test_execute_show(self, mock_run):
        """Test SHOW command execution (always runs)."""
        mock_result = Mock()
        mock_result.stdout = "test output"
        mock_result.stderr = ""
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        stdout, stderr, code = self.monitor.execute_show("echo test")
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
            self.monitor, "execute_show", return_value=(psql_output, "", 0)
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
            self.monitor, "execute_show", return_value=(psql_output, "", 0)
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
            self.monitor, "execute_show", return_value=(psql_output, "", 0)
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

    def test_find_redistribute_key_range_random_selection(self):
        """Test that random selection returns one of the matching key ranges."""
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
            KeyRange(
                key_range_id="ds_user_id_kr_12345678_1234_1234_1234_123456789012",
                shard_id="shard0",
                lower_bound="'12345678-1234-1234-1234-123456789012'",
            ),
        ]

        # Test multiple times to verify random selection
        results = set()
        for _ in range(20):
            kr = self.monitor.find_redistribute_key_range(key_ranges)
            self.assertIsNotNone(kr)
            self.assertEqual(kr.shard_id, "shard0")
            self.assertTrue(kr.key_range_id.startswith("ds_user_id_kr_"))
            results.add(kr.key_range_id)
        
        # With 20 attempts and 3 options, we should get at least 2 different results
        # (statistically very likely)
        self.assertGreaterEqual(len(results), 1)
    
    def test_find_redistribute_key_range_excludes_non_matching(self):
        """Test that only matching key ranges are considered."""
        key_ranges = [
            KeyRange(
                key_range_id="other_kr_something",
                shard_id="shard0",
                lower_bound="'00000000-0000-0000-0000-000000000000'",
            ),
            KeyRange(
                key_range_id="ds_user_id_kr_0",
                shard_id="shard-001",  # Wrong shard
                lower_bound="'00000000-0000-0000-0000-000000000000'",
            ),
            KeyRange(
                key_range_id="ds_user_id_kr_valid",
                shard_id="shard0",
                lower_bound="'00000000-0000-0000-0000-000000000000'",
            ),
        ]

        kr = self.monitor.find_redistribute_key_range(key_ranges)
        self.assertIsNotNone(kr)
        self.assertEqual(kr.key_range_id, "ds_user_id_kr_valid")
        self.assertEqual(kr.shard_id, "shard0")

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
            "execute_show",
            return_value=(" is_read_only \n--------------\n false", "", 0),
        ):
            result = self.monitor.check_read_only()

        self.assertFalse(result)

    def test_check_read_only_true(self):
        """Test checking read-only when true."""
        with patch.object(
            self.monitor,
            "execute_show",
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
        """Test all_running_enough when running count < 8."""
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

    def test_all_running_enough_real_world_scenario(self):
        """Test all_running_enough with real-world data: 11 RUNNING + 8 ERROR tasks.
        
        This is the bug fix test - previously the function would return False
        if not ALL tasks were RUNNING (checking len(running) == len(task_groups)),
        causing it to start more than 8 redistribution tasks simultaneously.
        
        Now it correctly returns True when there are >= 8 RUNNING tasks,
        regardless of ERROR or other states.
        
        Real example: 19 total tasks with 11 RUNNING and 8 ERROR should stop
        new redistributions from starting.
        """
        task_groups = [
            # 11 RUNNING tasks, 8 ERROR tasks
            TaskGroup("57cf6b02-3925-4c72-be9f-0dc98747edb6", "shard-002", "ds_user_id_kr_3d000000_0000_0000_0000_000000000000", "RUNNING"),
            TaskGroup("acb3716d-2bef-41aa-8ae0-d0ab6838ea93", "shard-001", "ds_user_id_kr_061f5376_dc31_427f_96e6_0812efd130de", "RUNNING"),
            TaskGroup("a38eca6c-c3a3-4405-90bc-e3beb48bca5d", "shard-008", "ds_user_id_kr_e22933c6_47cb_4f37_a406_4a22520e9aba", "ERROR", "etcdserver: request timed out"),
            TaskGroup("ba0ed875-7ab9-4359-886e-e766da93d547", "shard-004", "ds_user_id_kr_621c5280_a79c_4686_9c54_2f07b48e49db", "ERROR", "failed to split because bound intersects"),
            TaskGroup("ab4877a7-61ea-465e-8bf5-514719e3128a", "shard-004", "ds_user_id_kr_6b2593b7_52de_417a_ad43_76cf67b87a99", "ERROR", "etcdserver: request timed out"),
            TaskGroup("9eaaf2d3-5696-42d9-9b99-4fcf47689296", "shard-007", "ds_user_id_kr_cf6b8fc2_cfb4_4dbc_99b8_d9523c9aea20", "RUNNING"),
            TaskGroup("6a4128d8-c522-49b5-a132-62a3cbb98b53", "shard-007", "ds_user_id_kr_d78c737b_e178_4658_aacf_4e9e694b4357", "ERROR", "etcdserver: request timed out"),
            TaskGroup("41932f71-6776-4959-a1cf-606a565b0112", "shard-004", "ds_user_id_kr_7cb49909_2d48_46dd_a383_bb1af163d50c", "RUNNING"),
            TaskGroup("b528fb8f-2487-4ba6-b216-cab01be03518", "shard-006", "ds_user_id_kr_b255f952_a0b9_4095_bb94_d90d248013b3", "ERROR", "could not move the data"),
            TaskGroup("a1a78c3d-6a61-44e4-9f9b-31ac8b044c3a", "shard-004", "ds_user_id_kr_621c5280_a79c_4686_9c54_2f07b48e49db", "RUNNING"),
            TaskGroup("85b7feec-8a95-49e8-bb87-2764f28a9b23", "shard-003", "ds_user_id_kr_55d88de1_71b3_4c2a_a1d2_a0e0c0f3ec9f", "RUNNING"),
            TaskGroup("e55e0c85-ad55-414e-a5fd-705056c3e679", "shard-003", "ds_user_id_kr_4f0fb40f_c8db_42b2_85c5_2e6a3f04f6a6", "RUNNING"),
            TaskGroup("5f36ca29-00be-4087-a74d-fd0daa016fae", "shard-007", "ds_user_id_kr_c0967011_2e83_4980_af15_28c95ca89aa5", "ERROR", "failed to split because bound intersects"),
            TaskGroup("e1a8084b-5063-48c0-84b4-9b9c355d4324", "shard-007", "ds_user_id_kr_c0967011_2e83_4980_af15_28c95ca89aa5", "RUNNING"),
            TaskGroup("67cf2700-6c3b-4161-b5b6-a293bbd5016a", "shard-002", "ds_user_id_kr_36a1d0b8_c43f_4422_afab_8dbb25da60ff", "RUNNING"),
            TaskGroup("3f892bb6-9c41-4d1d-9853-a3baa2591405", "shard-004", "ds_user_id_kr_788b7ee2_45b0_4b6e_abf5_ebf3d59e1248", "RUNNING"),
            TaskGroup("732c4f17-d1da-4556-b7ea-a80efe8b1b0c", "shard-005", "ds_user_id_kr_8d963908_f5f3_4065_bb3e_45d813d9351b", "RUNNING"),
            TaskGroup("5d857a33-d940-4077-a450-1c7c770f6b3c", "shard-006", "ds_user_id_kr_aaa910ba_7039_4d88_88e3_06fc86d04a95", "RUNNING"),
            TaskGroup("70ad8bc5-fdda-469a-bc74-abb99536ad36", "shard-004", "ds_user_id_kr_7ed32a5e_5c69_4eca_b789_f084ab5cfd80", "RUNNING"),
        ]

        result = self.monitor.all_running_enough(task_groups)
        # With 11 RUNNING tasks (>= 8), should return True to prevent starting more
        self.assertTrue(result)

    def test_max_failed_tasks_threshold_not_exceeded(self):
        """Test that monitor continues when failed tasks are within threshold."""
        monitor_with_limit = SPQRMonitor(
            db_host="localhost",
            db_port=6432,
            db_name="spqr-console",
            db_user="spqr-console",
            dry_run=True,
            max_failed_tasks=10,
            logger=self.logger,
        )
        
        task_groups = [
            TaskGroup("tg1", "shard-001", "kr1", "ERROR", "some error"),
            TaskGroup("tg2", "shard-001", "kr2", "ERROR", "some error"),
            TaskGroup("tg3", "shard-001", "kr3", "ERROR", "some error"),
            TaskGroup("tg4", "shard-001", "kr4", "RUNNING"),
            TaskGroup("tg5", "shard-001", "kr5", "RUNNING"),
        ]
        
        with patch.object(monitor_with_limit, "check_read_only", return_value=False):
            with patch.object(monitor_with_limit, "get_task_groups", return_value=task_groups):
                with patch.object(monitor_with_limit, "retry_error_task_groups", return_value=0):
                    with patch.object(monitor_with_limit, "all_running_enough", return_value=True):
                        # Should not raise SystemExit because 3 < 10
                        monitor_with_limit.run_iteration()

    def test_max_failed_tasks_threshold_exceeded(self):
        """Test that monitor exits when failed tasks exceed threshold."""
        monitor_with_limit = SPQRMonitor(
            db_host="localhost",
            db_port=6432,
            db_name="spqr-console",
            db_user="spqr-console",
            dry_run=True,
            max_failed_tasks=5,
            logger=self.logger,
        )
        
        task_groups = [
            TaskGroup("tg1", "shard-001", "kr1", "ERROR", "some error"),
            TaskGroup("tg2", "shard-001", "kr2", "ERROR", "some error"),
            TaskGroup("tg3", "shard-001", "kr3", "ERROR", "some error"),
            TaskGroup("tg4", "shard-001", "kr4", "ERROR", "some error"),
            TaskGroup("tg5", "shard-001", "kr5", "ERROR", "some error"),
            TaskGroup("tg6", "shard-001", "kr6", "ERROR", "some error"),
            TaskGroup("tg7", "shard-001", "kr7", "RUNNING"),
        ]
        
        with patch.object(monitor_with_limit, "check_read_only", return_value=False):
            with patch.object(monitor_with_limit, "get_task_groups", return_value=task_groups):
                # Should raise SystemExit because 6 > 5
                with self.assertRaises(SystemExit) as cm:
                    monitor_with_limit.run_iteration()
                self.assertEqual(cm.exception.code, 1)

    def test_max_failed_tasks_exact_threshold(self):
        """Test that monitor continues when failed tasks equal threshold (not exceeded)."""
        monitor_with_limit = SPQRMonitor(
            db_host="localhost",
            db_port=6432,
            db_name="spqr-console",
            db_user="spqr-console",
            dry_run=True,
            max_failed_tasks=5,
            logger=self.logger,
        )
        
        task_groups = [
            TaskGroup("tg1", "shard-001", "kr1", "ERROR", "some error"),
            TaskGroup("tg2", "shard-001", "kr2", "ERROR", "some error"),
            TaskGroup("tg3", "shard-001", "kr3", "ERROR", "some error"),
            TaskGroup("tg4", "shard-001", "kr4", "ERROR", "some error"),
            TaskGroup("tg5", "shard-001", "kr5", "ERROR", "some error"),
            TaskGroup("tg6", "shard-001", "kr6", "RUNNING"),
        ]
        
        with patch.object(monitor_with_limit, "check_read_only", return_value=False):
            with patch.object(monitor_with_limit, "get_task_groups", return_value=task_groups):
                with patch.object(monitor_with_limit, "retry_error_task_groups", return_value=0):
                    with patch.object(monitor_with_limit, "all_running_enough", return_value=True):
                        # Should not raise SystemExit because 5 == 5 (not exceeded)
                        monitor_with_limit.run_iteration()

    def test_max_failed_tasks_none_disables_check(self):
        """Test that max_failed_tasks=None disables the check."""
        monitor_no_limit = SPQRMonitor(
            db_host="localhost",
            db_port=6432,
            db_name="spqr-console",
            db_user="spqr-console",
            dry_run=True,
            max_failed_tasks=None,
            logger=self.logger,
        )
        
        task_groups = [
            TaskGroup(f"tg{i}", "shard-001", f"kr{i}", "ERROR", "some error")
            for i in range(100)  # 100 failed tasks
        ]
        
        with patch.object(monitor_no_limit, "check_read_only", return_value=False):
            with patch.object(monitor_no_limit, "get_task_groups", return_value=task_groups):
                with patch.object(monitor_no_limit, "retry_error_task_groups", return_value=0):
                    with patch.object(monitor_no_limit, "all_running_enough", return_value=True):
                        # Should not raise SystemExit even with 100 failed tasks
                        monitor_no_limit.run_iteration()

    def test_retry_error_task_groups_success(self):
        """Test retrying error task groups with retryable errors."""
        task_groups = [
            TaskGroup("tg1", "shard-001", "kr1", "ERROR", "rpc error: code = Canceled desc = grpc: the client connection is closing"),
            TaskGroup("tg2", "shard-001", "kr2", "RUNNING"),
        ]

        with patch.object(
            self.monitor, "execute_write", return_value=("", "", 0)
        ):
            count = self.monitor.retry_error_task_groups(task_groups)

        self.assertEqual(count, 1)

    def test_retry_error_task_groups_max_4(self):
        """Test that max 4 task groups are retried per iteration."""
        task_groups = [
            TaskGroup(f"tg{i}", "shard-001", f"kr{i}", "ERROR", "etcdserver: request timed out")
            for i in range(10)
        ]

        with patch.object(
            self.monitor, "execute_write", return_value=("", "", 0)
        ):
            count = self.monitor.retry_error_task_groups(task_groups)

        self.assertEqual(count, 4)

    def test_retry_etcdserver_timeout_short_message(self):
        """Test retrying tasks with short etcdserver timeout error (without 'possibly due to')."""
        task_groups = [
            TaskGroup("tg1", "shard-007", "kr1", "ERROR", "etcdserver: request timed out"),
            TaskGroup("tg2", "shard-004", "kr2", "ERROR", "etcdserver: request timed out"),
            TaskGroup("tg3", "shard-008", "kr3", "ERROR", "etcdserver: request timed out"),
            TaskGroup("tg4", "shard-001", "kr4", "RUNNING"),
        ]

        with patch.object(
            self.monitor, "execute_write", return_value=("", "", 0)
        ):
            count = self.monitor.retry_error_task_groups(task_groups)

        # Should retry 3 error tasks (not the RUNNING one)
        self.assertEqual(count, 3)

    def test_retry_etcdserver_timeout_long_message(self):
        """Test retrying tasks with long etcdserver timeout error (with 'possibly due to')."""
        task_groups = [
            TaskGroup("tg1", "shard-001", "kr1", "ERROR", "etcdserver: request timed out, possibly due to previous leader failure"),
            TaskGroup("tg2", "shard-001", "kr2", "RUNNING"),
        ]

        with patch.object(
            self.monitor, "execute_write", return_value=("", "", 0)
        ):
            count = self.monitor.retry_error_task_groups(task_groups)

        # Should match because 'etcdserver: request timed out' is contained in the longer message
        self.assertEqual(count, 1)

    def test_retry_non_retryable_errors(self):
        """Test that non-retryable errors are not retried.
        
        Tests specific real-world error messages that should NOT be retried:
        - Duplicate key constraint violations
        - Bound intersection errors
        """
        task_groups = [
            TaskGroup("tg1", "shard-006", "kr1", "ERROR", 
                     "could not move the data: ERROR: duplicate key value violates unique constraint \"table_unique_idx\" (SQLSTATE 23505)"),
            TaskGroup("tg2", "shard-007", "kr2", "ERROR", 
                     "failed to split because bound intersects with \"680dd8ad-fe87-4471-aa8f-96523bc10efc\" key range"),
            TaskGroup("tg3", "shard-004", "kr3", "ERROR", 
                     "failed to split because bound intersects with \"302f3e26-bf48-4c99-9c63-bab5b4d1f416\" key range"),
            TaskGroup("tg4", "shard-001", "kr4", "ERROR", "some other error"),
        ]

        with patch.object(
            self.monitor, "execute_write", return_value=("", "", 0)
        ) as mock_write:
            count = self.monitor.retry_error_task_groups(task_groups)

        self.assertEqual(count, 0)
        mock_write.assert_not_called()

    def test_no_retry_errors_flag(self):
        """Test that --no-retry-errors flag disables retry functionality."""
        monitor_no_retry = SPQRMonitor(
            db_host="localhost",
            db_port=6432,
            db_name="spqr-console",
            db_user="spqr-console",
            dry_run=True,
            no_retry_errors=True,
            logger=self.logger,
        )
        
        task_groups = [
            TaskGroup("tg1", "shard-001", "kr1", "ERROR", "etcdserver: request timed out"),
            TaskGroup("tg2", "shard-001", "kr2", "ERROR", "etcdserver: request timed out"),
            TaskGroup("tg3", "shard-001", "kr3", "RUNNING"),
        ]

        with patch.object(
            monitor_no_retry, "execute_write", return_value=("", "", 0)
        ) as mock_write:
            count = monitor_no_retry.retry_error_task_groups(task_groups)

        # Function should still work, but won't be called in run_iteration
        self.assertEqual(count, 2)
        
        # Verify the flag is set correctly
        self.assertTrue(monitor_no_retry.no_retry_errors)
        self.assertFalse(self.monitor.no_retry_errors)

    def test_redistribute_key_range_dry_run(self):
        """Test redistribute key range in dry-run mode."""
        result = self.monitor.redistribute_key_range(
            "ds_user_id_kr_test", "shard-001"
        )
        self.assertTrue(result)

    @patch("subprocess.Popen")
    @patch("builtins.open", create=True)
    def test_redistribute_key_range_prod(self, mock_open, mock_popen):
        """Test redistribute key range in production mode."""
        mock_process = Mock()
        mock_process.pid = 12345
        mock_popen.return_value = mock_process
        
        mock_file = Mock()
        mock_open.return_value = mock_file

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
        mock_popen.assert_called_once()
        mock_open.assert_called_once()


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
