"""BaseClient lifecycle: route through the live engine, swap it for an inert
twin on close, and never raise into the host.
"""

from __future__ import annotations

from evpanda.client import BaseClient


class FakeEngine:
    def __init__(self, *, explode: bool = False) -> None:
        self.flushes = 0
        self.closes: list[float | None] = []
        self._explode = explode

    def flush(self) -> None:
        self.flushes += 1
        if self._explode:
            raise RuntimeError("boom")

    def close(self, deadline: float | None = None) -> None:
        self.closes.append(deadline)
        if self._explode:
            raise RuntimeError("boom")


class FakeClient(BaseClient[FakeEngine]):
    def __init__(self, engine: FakeEngine) -> None:
        self._noops: list[FakeEngine] = []
        super().__init__(engine, self._make_noop)

    def _make_noop(self) -> FakeEngine:
        noop = FakeEngine()
        self._noops.append(noop)
        return noop

    @property
    def engine(self) -> FakeEngine:
        return self._engine


def test_flush_routes_to_the_live_engine() -> None:
    engine = FakeEngine()
    client = FakeClient(engine)
    client.flush()
    client.flush()
    assert engine.flushes == 2


def test_close_swaps_in_an_inert_engine() -> None:
    engine = FakeEngine()
    client = FakeClient(engine)

    client.close(2.5)
    assert engine.closes == [2.5]
    assert client.engine is not engine

    # Post-close capture/flush hits the inert twin, never the closed engine.
    client.flush()
    assert engine.flushes == 0
    assert client.engine.flushes == 1


def test_close_is_idempotent() -> None:
    engine = FakeEngine()
    client = FakeClient(engine)
    client.close()
    client.close()
    client.close()
    assert engine.closes == [None]  # the original engine is closed exactly once


def test_engine_failures_are_swallowed() -> None:
    client = FakeClient(FakeEngine(explode=True))
    client.flush()  # must not raise
    client.close()  # must not raise
