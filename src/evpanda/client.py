"""Shared engine lifecycle for OCPIClient and OCPPClient: hold one engine,
swap it for an inert twin on ``close``, expose ``flush`` / ``close``. The
protocol-specific capture methods stay in the subclasses.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable
from typing import Protocol


class ClientEngine(Protocol):
    """The minimum an engine must expose for the base to drive its lifecycle."""

    def flush(self) -> None: ...

    def close(self, deadline: float | None = None) -> None: ...


class BaseClient[E: ClientEngine]:
    """Base for the protocol clients — internal; never instantiated directly."""

    def __init__(self, engine: E, make_noop: Callable[[], E]) -> None:
        self._lock = threading.Lock()
        self.__engine = engine
        #: Builds the inert engine `close` swaps in. Subclass supplies it.
        self.__make_noop = make_noop

    @property
    def _engine(self) -> E:
        """The live engine — subclass capture methods route through this."""
        with self._lock:
            return self.__engine

    def flush(self) -> None:
        """Force an immediate flush of buffered messages. Never raises."""
        with contextlib.suppress(Exception):
            self._engine.flush()

    def close(self, deadline: float | None = None) -> None:
        """Swap to inert then drain within the deadline (seconds). Idempotent."""
        with self._lock:
            engine = self.__engine
            self.__engine = self.__make_noop()
        with contextlib.suppress(Exception):
            engine.close(deadline)
