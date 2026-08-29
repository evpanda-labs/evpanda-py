"""Preforking servers: a child must deliver its own captures.

gunicorn, uWSGI and friends fork their workers out of a parent that has
already imported the application — and with it, often, the SDK client. Only
the forking thread survives a fork, so without the after-fork hook the
child would capture into a buffer nothing ever drains.
"""

from __future__ import annotations

import os

import pytest

from conftest import PARTNER, IngestServer, exchange, ocpi_client

pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is POSIX-only")


def test_a_forked_child_delivers_its_own_captures(ingest: IngestServer) -> None:
    panda = ocpi_client(ingest, flush_interval=3600.0)
    panda.capture_inbound_message(PARTNER, exchange(url="/parent"))

    pid = os.fork()
    if pid == 0:  # the child
        status = 0
        try:
            panda.capture_inbound_message(PARTNER, exchange(url="/child"))
            if panda.stats().captured != 1:  # the parent's tally does not carry over
                status = 2
            if not panda.close(timeout=10):
                status = 3
        except BaseException:  # noqa: BLE001 - the child reports through its exit code
            status = 4
        os._exit(status)

    assert os.waitstatus_to_exitcode(os.waitpid(pid, 0)[1]) == 0
    assert panda.close(timeout=10) is True

    urls = [message["url"] for message in ingest.messages]
    assert sorted(urls) == ["/child", "/parent"]
