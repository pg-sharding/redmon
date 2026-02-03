#!/usr/bin/env python3
"""
Integration tests demonstrating monitor behavior.
"""

import unittest
from unittest.mock import patch, Mock
from spqr_monitor import SPQRMonitor
import logging


class TestIntegration(unittest.TestCase):
    """Integration tests for full monitor workflow."""

    def setUp(self):
        """Set up test fixtures."""
        self.logger = logging.getLogger("integration_test")
        self.monitor = SPQRMonitor(
            db_host="localhost",
            db_port=6432,
            db_name="spqr-console",
            db_user="spqr-console",
            dry_run=True,
            logger=self.logger,
        )

    def test_full_iteration_with_errors(self):
        """Test full iteration with error task groups."""
        # Mock response: database is not read-only
        read_only_response = " is_read_only \n--------------\n false"

        # Mock response: some task groups with errors
        task_groups_response = """task_group_id | destination_shard_id | source_key_range_id | state | error
----+---+---+---+---
 tg1 | shard-001 | kr1 | ERROR | rpc error: connection closed
 tg2 | shard-002 | kr2 | RUNNING |"""

        # Mock response: key ranges including shard0
        key_ranges_response = """key_range_id | shard_id | distribution_id | lower_bound | locked
---+---+---+---+---
 ds_user_id_kr_test1 | shard0 | ds_user_id | '00000000-0000-0000-0000-000000000001' | false
 kr_on_shard1 | shard-001 | ds_user_id | '00000000-0000-0000-0000-000000000002' | false"""

        with patch.object(
            self.monitor, "execute_show"
        ) as mock_show, patch.object(
            self.monitor, "execute_write"
        ) as mock_write:
            # Configure mock to return different responses for different calls
            mock_show.side_effect = [
                (read_only_response, "", 0),  # check_read_only
                (task_groups_response, "", 0),  # get_task_groups
                (task_groups_response, "", 0),  # refresh task_groups
                (key_ranges_response, "", 0),  # get_key_ranges
            ]
            mock_write.return_value = ("", "", 0)  # retry_error_task_groups

            self.monitor.run_iteration()

        # Verify methods were called
        self.assertGreater(mock_show.call_count, 0)
        self.assertGreater(mock_write.call_count, 0)

    def test_full_iteration_all_running(self):
        """Test full iteration when all task groups are RUNNING and count < 8."""
        # Mock response: database is not read-only
        read_only_response = " is_read_only \n--------------\n false"

        # Mock response: some task groups are RUNNING (less than 8)
        # This will trigger key range redistribution
        task_groups_response = """task_group_id | destination_shard_id | source_key_range_id | state | error
----+---+---+---+---
 tg1 | shard-001 | kr1 | RUNNING |
 tg2 | shard-001 | kr2 | RUNNING |"""

        key_ranges_response = """key_range_id | shard_id | distribution_id | lower_bound | locked
----+---+---+---+---
 ds_user_id_kr_test1 | shard0 | ds_user_id | 'bound1' | false"""

        with patch.object(
            self.monitor, "execute_show"
        ) as mock_show:
            mock_show.side_effect = [
                (read_only_response, "", 0),  # check_read_only
                (task_groups_response, "", 0),  # get_task_groups (first call)
                (task_groups_response, "", 0),  # get_task_groups (second call)
                (key_ranges_response, "", 0),  # get_key_ranges
            ]

            self.monitor.run_iteration()

        # Should call multiple times
        self.assertGreaterEqual(mock_show.call_count, 3)

    def test_full_iteration_read_only(self):
        """Test full iteration when database is in read-only mode."""
        # Mock response: database is read-only
        read_only_response = " is_read_only \n--------------\n true"

        with patch.object(
            self.monitor, "execute_show"
        ) as mock_show:
            mock_show.return_value = (read_only_response, "", 0)
            self.monitor.run_iteration()

        # Should only call check_read_only
        self.assertEqual(mock_show.call_count, 1)

    def test_dry_run_mode_commands(self):
        """Test that dry-run mode prints commands without executing."""
        import io
        import sys

        # Capture stdout
        captured_output = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            monitor = SPQRMonitor(
                db_host="localhost",
                db_port=6432,
                db_name="spqr-console",
                db_user="spqr-console",
                dry_run=True,
                logger=self.logger,
            )

            # Execute a command
            stdout, stderr, code = monitor.execute_write("echo test")

            # Check output
            output = captured_output.getvalue()
            self.assertIn("echo test", output)
            self.assertEqual(code, 0)

        finally:
            sys.stdout = old_stdout


if __name__ == "__main__":
    unittest.main()
