"""Reads the streaming job's output tables straight off HDFS.

Not via Hive JDBC/Thrift: HiveServer2 2.3.2's SASL handshake doesn't match
what modern Python Thrift clients (pyhive/thrift-sasl) expect -- the same
class of old-library-vs-new-client mismatch that already broke Bitnami's
Kafka tag, kafka-python on 3.12, and would have broken Spark's
enableHiveSupport too (see streaming/divergence_job.py's docstring for that
one). Debugging a Thrift/SASL protocol mismatch isn't worth it when the
data's real location -- HDFS -- is directly reachable.

Not via WebHDFS's HTTP API either: WebHDFS's file-read redirects to a
datanode's own address, and the datanode container isn't port-published to
the host, so that redirect wouldn't resolve outside the Docker network.

Instead: `docker exec` a plain `hdfs dfs -get` (proven reliable everywhere
else in this project) to pull the current Parquet files onto the host, then
read them with pandas. Simple, and every failure mode here is a normal
subprocess/file error rather than a protocol-version guessing game.
"""
from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

import pandas as pd

NAMENODE_CONTAINER = "linewatch-namenode"
HDFS_BASE = "/data/line_watch"


def read_table(table_name: str) -> pd.DataFrame:
    """Pulls every *.parquet file under HDFS_BASE/table_name and concatenates
    them. Returns an empty DataFrame (no error) if the table doesn't exist
    yet -- the job may not have produced output yet on a fresh stack."""
    with tempfile.TemporaryDirectory() as tmpdir:
        container_tmp = f"/tmp/{table_name}_dashboard_pull"
        hdfs_path = f"{HDFS_BASE}/{table_name}"

        subprocess.run(
            ["docker", "exec", NAMENODE_CONTAINER, "bash", "-c", f"rm -rf {container_tmp}"],
            capture_output=True,
        )
        get_result = subprocess.run(
            ["docker", "exec", NAMENODE_CONTAINER, "hdfs", "dfs", "-get", hdfs_path, container_tmp],
            capture_output=True,
            text=True,
        )
        if get_result.returncode != 0:
            return pd.DataFrame()  # table not created yet

        subprocess.run(
            ["docker", "cp", f"{NAMENODE_CONTAINER}:{container_tmp}", str(Path(tmpdir) / table_name)],
            capture_output=True,
        )
        subprocess.run(
            ["docker", "exec", NAMENODE_CONTAINER, "bash", "-c", f"rm -rf {container_tmp}"],
            capture_output=True,
        )

        local_dir = Path(tmpdir) / table_name
        parquet_files = sorted(local_dir.glob("*.parquet")) if local_dir.exists() else []
        if not parquet_files:
            return pd.DataFrame()

        return pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
