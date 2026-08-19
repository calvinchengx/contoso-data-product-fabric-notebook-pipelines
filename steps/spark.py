"""The Spark session, wherever this runs.

WHY SPARK AND NOT DUCKDB. bronze and silver are the distributed half of the
medallion. A single-node engine is the right tool for a Fabric *Python*
notebook and the wrong one for a platform that has to scale — so the transforms
are Spark, and the same code is a Spark notebook or a Spark Job Definition in
production.

WHERE THE SESSION COMES FROM. Inside a Fabric notebook `spark` already exists;
the platform must not build a second one. Outside a notebook — a laptop, CI, a
container — you connect to a Spark Connect endpoint. Both are the same
`SparkSession` afterwards, so nothing downstream knows the difference.

`pyspark-client` is the thin Connect client: about a megabyte, no JVM. The
engine is Sail here and a Fabric Spark pool in production.
"""

from __future__ import annotations

import os

# QUIET THE TRANSPORT, not the engine. Spark Connect talks gRPC, and gRPC's
# C-core logs at INFO by default — so every step that holds a session and then
# spawns a subprocess emits a wall of
# `ev_poll_posix.cc:593] FD from fork parent still in poll list`, one line per
# inherited descriptor. It is not a warning about anything: the child is being
# told about file descriptors it will never use.
#
# Measured on `govern.py`: 14 lines of it per run, arriving in a block that
# buries the step's actual output. In a recorded demo it buries the whole
# terminal pane.
#
# ERROR rather than NONE, because a transport that genuinely fails should still
# say so — the noise worth removing is the INFO chatter, not the diagnostics.
# Set before pyspark is imported: the C-core reads this when it initialises.
os.environ.setdefault("GRPC_VERBOSITY", "ERROR")

from fabric import T
from pyspark.sql import SparkSession


def session() -> SparkSession:
    # An ambient session wins. In a Fabric notebook one is already running with
    # the workspace identity and the default lakehouse attached, and connecting
    # somewhere else from inside it would be both wrong and slower.
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    if not T.spark_remote:
        raise SystemExit(
            "no Spark session and no SPARK_REMOTE. Inside a Fabric notebook a "
            "session is ambient; outside one, point SPARK_REMOTE at a Spark "
            "Connect endpoint."
        )
    return SparkSession.builder.remote(T.spark_remote).getOrCreate()


def lakehouse_uri(workspace: str, lakehouse: str) -> str:
    """The OneLake path Spark reads and writes.

    The real Fabric scheme on both targets — `abfs://{workspace}@onelake…` —
    because it is the engine that resolves it, not this process. Sail is
    configured with the emulator's storage endpoint and reaches the same store.
    """
    return f"abfs://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}"
