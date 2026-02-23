#!/usr/bin/env python3
"""
SPQR key range redistribution monitor.
Monitors task groups and redistributes key ranges between shards.
"""

import argparse
import subprocess
import logging
import logging.handlers
import time
import re
import sys
import random
import hashlib
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass


@dataclass
class KeyRange:
    """Represents a key range."""
    key_range_id: str
    shard_id: str
    lower_bound: str


@dataclass
class TaskGroup:
    """Represents a task group."""
    task_group_id: str
    destination_shard_id: str
    source_key_range_id: str
    state: str
    error: Optional[str] = None


class SPQRMonitor:
    """Monitor for SPQR key range redistribution."""

    def __init__(
        self,
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        dry_run: bool = False,
        retry_errors: bool = False,
        max_failed_tasks: Optional[int] = None,
        max_running_tasks: int = 8,
        max_retries_per_iteration: int = 1,
        logger: Optional[logging.Logger] = None,
    ):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.dry_run = dry_run
        self.retry_errors = retry_errors
        self.max_failed_tasks = max_failed_tasks
        self.max_running_tasks = max_running_tasks
        self.max_retries_per_iteration = max_retries_per_iteration
        self.logger = logger or self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup default logger."""
        logger = logging.getLogger("spqr_monitor")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
        return logger

    def _psql_command(self, sql: str) -> str:
        """Build psql command."""
        return (
            f'/usr/bin/psql "port={self.db_port} dbname={self.db_name} '
            f'user={self.db_user}" -c "{sql}"'
        )

    def execute_show(self, cmd: str) -> Tuple[str, str, int]:
        """Execute SHOW command. Always executes regardless of dry-run mode."""
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=30
            )
            return result.stdout, result.stderr, result.returncode
        except subprocess.TimeoutExpired:
            self.logger.error(f"Command timeout: {cmd}")
            return "", "Timeout", 1
        except Exception as e:
            self.logger.error(f"Command error: {e}")
            return "", str(e), 1

    def execute_write(self, cmd: str) -> Tuple[str, str, int]:
        """Execute write command. In dry-run mode, only prints. Runs in separate process with 25h timeout."""
        if self.dry_run:
            print(cmd)
            return "", "", 0

        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=90000
            )
            return result.stdout, result.stderr, result.returncode
        except subprocess.TimeoutExpired:
            self.logger.error(f"Command timeout: {cmd}")
            return "", "Timeout", 1
        except Exception as e:
            self.logger.error(f"Command error: {e}")
            return "", str(e), 1

    def check_read_only(self) -> bool:
        """Check if database is in read-only mode."""
        cmd = self._psql_command("SHOW is_read_only;")
        stdout, stderr, code = self.execute_show(cmd)

        if code != 0:
            self.logger.error(f"Failed to check read-only status: {stderr}")
            return False

        if "true" in stdout.lower():
            return True

        return False

    def get_task_groups(self) -> List[TaskGroup]:
        """Get all task groups from database."""
        cmd = self._psql_command("SHOW task_group;")
        stdout, stderr, code = self.execute_show(cmd)

        if code != 0:
            self.logger.error(f"Failed to get task groups: {stderr}")
            return []

        task_groups = []
        for line in stdout.split("\n"):
            line = line.strip()
            if not line or line.startswith("-") or "task_group_id" in line:
                continue

            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 7:
                continue

            task_groups.append(
                TaskGroup(
                    task_group_id=parts[0],
                    destination_shard_id=parts[1],
                    source_key_range_id=parts[2],
                    state=parts[6],
                    error=parts[7] if len(parts) > 7 and parts[7] else None,
                )
            )

        return task_groups

    def _is_retryable_error(self, error: Optional[str]) -> bool:
        """Check if error is retryable."""
        if not error:
            return False
        
        retryable_errors = [
            "etcdserver: request timed out",
            "rpc error: code = Canceled desc = grpc: the client connection is closing",
            "task group lost running",
        ]
        
        return any(retryable in error for retryable in retryable_errors)

    def _execute_psql_async(self, sql_cmd: str, operation_name: str, identifier: str) -> bool:
        """Execute a psql command as background process.
        
        Args:
            sql_cmd: The SQL command to execute
            operation_name: Name of the operation for logging (e.g., 'retry', 'redistribution')
            identifier: Unique identifier for creating log file hash
        
        Returns:
            True if background process started successfully, False otherwise
        """
        # Create short identifier for log files
        session_hash = hashlib.md5(identifier.encode()).hexdigest()[:8]
        log_file = f"/var/log/spqr/{operation_name}_{session_hash}.log"
        
        # Build psql command with individual flags
        psql_args = [
            "/usr/bin/psql",
            "-p", str(self.db_port),
            "-d", self.db_name,
            "-U", self.db_user,
            "-c", sql_cmd
        ]
        
        if self.dry_run:
            cmd_str = " ".join([f'"{arg}"' if " " in arg else arg for arg in psql_args])
            print(f"Would run: {cmd_str} > {log_file} 2>&1 &")
            return True
        
        try:
            # Open log file for output
            log_fd = open(log_file, "w")
            
            # Start background process
            process = subprocess.Popen(
                psql_args,
                stdout=log_fd,
                stderr=subprocess.STDOUT,
                start_new_session=True  # Detach from parent session
            )
            
            self.logger.info(
                f"Started {operation_name} of {identifier} "
                f"(PID: {process.pid}, log: {log_file})"
            )
            
            # Don't wait for process to complete - it's a background task
            return True
            
        except Exception as e:
            self.logger.error(
                f"Failed to start {operation_name} for {identifier}: {e}"
            )
            return False

    def retry_task_group_async(self, task_group_id: str) -> bool:
        """Retry a task group as background process."""
        sql_cmd = f"RETRY TASK GROUP '{task_group_id}';"
        return self._execute_psql_async(sql_cmd, "retry", task_group_id)

    def retry_error_task_groups(self, task_groups: List[TaskGroup]) -> int:
        """Retry task groups with ERROR state and retryable errors."""
        error_groups = [
            tg for tg in task_groups 
            if tg.state == "ERROR" and self._is_retryable_error(tg.error)
        ]

        if not error_groups:
            return 0
        
        self.logger.info(f"Found {len(error_groups)} task groups with retryable errors")

        retry_count = 0
        for tg in error_groups[:self.max_retries_per_iteration]:
            if self.retry_task_group_async(tg.task_group_id):
                retry_count += 1

        return retry_count

    def all_running_enough(self, task_groups: List[TaskGroup]) -> bool:
        """Check if there are enough RUNNING task groups based on threshold."""
        running = [tg for tg in task_groups if tg.state == "RUNNING"]

        if len(running) >= self.max_running_tasks:
            self.logger.info(f"Already have {len(running)} RUNNING task groups (>= {self.max_running_tasks})")
            return True

        return False

    def get_key_ranges(self) -> List[KeyRange]:
        """Get all key ranges from database."""
        cmd = self._psql_command("SHOW key_ranges;")
        stdout, stderr, code = self.execute_show(cmd)

        if code != 0:
            self.logger.error(f"Failed to get key ranges: {stderr}")
            return []

        key_ranges = []
        for line in stdout.split("\n"):
            line = line.strip()
            if not line or line.startswith("-") or "key_range_id" in line:
                continue

            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 3:
                continue

            key_ranges.append(
                KeyRange(
                    key_range_id=parts[0],
                    shard_id=parts[1],
                    lower_bound=parts[3] if len(parts) > 3 else "",
                )
            )

        return key_ranges

    def find_redistribute_key_range(self, key_ranges: List[KeyRange]) -> Optional[KeyRange]:
        """Find a random key range matching pattern ds_user_id_kr_* from shard0."""
        matching = [
            kr for kr in key_ranges
            if kr.key_range_id.startswith("ds_user_id_kr_") and kr.shard_id == "shard0"
        ]
        
        if not matching:
            return None
        
        return random.choice(matching)

    def determine_target_shard(self, lower_bound: str) -> Optional[str]:
        """Determine target shard based on UUID lower_bound."""
        # Extract UUID from lower_bound (format: '00000000-0000-0000-0000-000000000000')
        uuid_match = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', lower_bound, re.IGNORECASE)
        if not uuid_match:
            self.logger.warning(f"Invalid UUID in lower_bound: {lower_bound}")
            return None
        
        uuid_str = uuid_match.group(1).lower()
        first_hex = uuid_str[0]
        
        # Map first hex digit to shard: 0-1→001, 2-3→002, ..., e-f→008
        hex_to_shard = {
            '0': 'shard-001', '1': 'shard-001',
            '2': 'shard-002', '3': 'shard-002',
            '4': 'shard-003', '5': 'shard-003',
            '6': 'shard-004', '7': 'shard-004',
            '8': 'shard-005', '9': 'shard-005',
            'a': 'shard-006', 'b': 'shard-006',
            'c': 'shard-007', 'd': 'shard-007',
            'e': 'shard-008', 'f': 'shard-008',
        }
        
        return hex_to_shard.get(first_hex)

    def redistribute_key_range(
        self, key_range_id: str, target_shard: str, batch_size: int = 300000
    ) -> bool:
        """Redistribute key range to target shard as background process."""
        sql_cmd = f"REDISTRIBUTE KEY RANGE '{key_range_id}' TO '{target_shard}' BATCH SIZE {batch_size};"
        return self._execute_psql_async(sql_cmd, "redistribute", key_range_id)

    def run_iteration(self) -> None:
        """Run one iteration of monitoring."""
        self.logger.info("Starting iteration")

        # Check read-only
        if self.check_read_only():
            self.logger.warning("Database is in read-only mode, skipping iteration")
            return

        # Get and retry error task groups
        task_groups = self.get_task_groups()
        self.logger.info(f"Found {len(task_groups)} task groups")

        failed_tasks = [tg for tg in task_groups if tg.state == "ERROR"]
        failed_count = len(failed_tasks)
        self.logger.info(f"Failed tasks count: {failed_count}")

        # Check if failed tasks exceed threshold
        if self.max_failed_tasks is not None:
            if failed_count > self.max_failed_tasks:
                self.logger.error(f"Failed tasks exceeded threshold ({self.max_failed_tasks}). Skipping iteration.")
                return

        if self.retry_errors:
            retry_count = self.retry_error_task_groups(task_groups)
            if retry_count > 0:
                self.logger.info(f"Retried {retry_count} task groups, skipping redistribution this iteration")
                return
            self.logger.info("No error task groups to retry, proceeding to redistribution check")
            # Refresh task groups after retry
            task_groups = self.get_task_groups()
        else:
            self.logger.info("Error retry disabled, proceeding to redistribution check")

        # Check if all running
        if self.all_running_enough(task_groups):
            self.logger.info("Skipping redistribution: enough tasks already running")
            return

        # Get key ranges and find one to redistribute
        key_ranges = self.get_key_ranges()
        self.logger.info(f"Found {len(key_ranges)} key ranges")

        kr = self.find_redistribute_key_range(key_ranges)
        if not kr:
            self.logger.info("No key ranges to redistribute from shard0")
            return

        target_shard = self.determine_target_shard(kr.lower_bound)
        if not target_shard:
            self.logger.warning("Could not determine target shard")
            return

        self.logger.info(
            f"Redistributing {kr.key_range_id} from {kr.shard_id} to {target_shard}"
        )
        self.redistribute_key_range(kr.key_range_id, target_shard)

    def run(self, iteration_timeout: int) -> None:
        """Run monitoring loop."""
        self.logger.info(
            f"Starting SPQR monitor (dry_run={self.dry_run}, timeout={iteration_timeout}s)"
        )

        try:
            while True:
                self.run_iteration()
                time.sleep(iteration_timeout)
        except KeyboardInterrupt:
            self.logger.info("Monitor stopped by user")


def setup_logging(log_file: Optional[str] = None) -> logging.Logger:
    """Setup logging to both file and stdout."""
    logger = logging.getLogger("spqr_monitor")
    logger.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10485760, backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


def main():
    parser = argparse.ArgumentParser(
        description="SPQR key range redistribution monitor"
    )
    parser.add_argument(
        "--db-host", default="localhost", help="Database host (default: localhost)"
    )
    parser.add_argument(
        "--db-port", type=int, default=6432, help="Database port (default: 6432)"
    )
    parser.add_argument(
        "--db-name", default="spqr-console", help="Database name (default: spqr-console)"
    )
    parser.add_argument(
        "--db-user", default="spqr-console", help="Database user (default: spqr-console)"
    )
    parser.add_argument(
        "--iteration-timeout",
        type=int,
        default=60,
        help="Iteration timeout in seconds (default: 60)",
    )
    parser.add_argument(
        "--log-file",
        default="./spqr_monitor.log",
        help="Log file path (default: ./spqr_monitor.log)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry-run mode: only print commands without executing",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Enable automatic retry of error task groups",
    )
    parser.add_argument(
        "--max-failed-tasks",
        type=int,
        default=10,
        help="Exit if number of failed tasks exceeds this threshold (default: 10)",
    )
    parser.add_argument(
        "--max-running-tasks",
        type=int,
        default=8,
        help="Skip redistribution if this many task groups are already running (default: 8)",
    )
    parser.add_argument(
        "--max-retries-per-iteration",
        type=int,
        default=1,
        help="Maximum number of task groups to retry per iteration (default: 1)",
    )

    args = parser.parse_args()

    logger = setup_logging(args.log_file)

    monitor = SPQRMonitor(
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_user=args.db_user,
        dry_run=args.dry_run,
        retry_errors=args.retry_errors,
        max_failed_tasks=args.max_failed_tasks,
        max_running_tasks=args.max_running_tasks,
        max_retries_per_iteration=args.max_retries_per_iteration,
        logger=logger,
    )

    monitor.run(args.iteration_timeout)


if __name__ == "__main__":
    main()
