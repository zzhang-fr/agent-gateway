# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Optional ``start()`` and ``stop()`` hooks of Gateway extension packages.

The object a package's ``create_extension()`` returns may define ``start()`` and
``stop()``, plain or ``async``. The runtime calls ``start()`` once the package is
published and ``stop()`` once its activation is released: uninstall, switch-off,
version switch and Gateway shutdown. Each hook runs at most once per load. The
hooks of one package run in order, so the old version's ``stop()`` ends before the
new version's ``start()`` begins.

Each call has a time limit. An ``async`` hook runs on the Gateway's event loop and
is cancelled at the limit. A plain hook runs in a daemon thread of its own, so a
slow hook cannot block the loop; at the limit the Gateway logs a warning and goes
on. A hook that raises is logged and fails no operation.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any

logger = logging.getLogger(__name__)

START = "start"
STOP = "stop"


class PackageLifecycle:
    """Runs package hooks in order per package, each within ``timeout`` seconds."""

    def __init__(self, timeout: float) -> None:
        self._timeout = timeout
        # The last scheduled hook run of each package; a new run waits for it.
        self._tails: dict[str, asyncio.Task[None]] = {}

    def schedule(self, package: Any, hook: str) -> None:
        """Run ``hook`` on every extension object of ``package`` after its earlier hooks."""

        calls = _hooks(package, hook)
        if not calls:
            return
        package_id = package.package_id
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            for call in calls:
                self._call_blocking(package_id, hook, call)
            return
        task = loop.create_task(self._run(self._tails.get(package_id), package_id, hook, calls))
        self._tails[package_id] = task

        def forget(done: asyncio.Task[None]) -> None:
            if self._tails.get(package_id) is done:
                del self._tails[package_id]

        task.add_done_callback(forget)

    async def settle(self, package_id: str) -> None:
        """Wait until every hook scheduled so far for ``package_id`` has ended."""

        tail = self._tails.get(package_id)
        if tail is not None:
            await asyncio.wait({tail})

    async def settle_all(self) -> None:
        """Wait until every scheduled hook has ended."""

        while self._tails:
            await asyncio.wait(set(self._tails.values()))

    async def _run(self, previous: asyncio.Task[None] | None, package_id: str, hook: str,
                   calls: Sequence[Callable[[], Any]]) -> None:
        if previous is not None:
            await asyncio.wait({previous})
        for call in calls:
            await self._call(package_id, hook, call)

    async def _call(self, package_id: str, hook: str, call: Callable[[], Any]) -> None:
        deadline = time.monotonic() + self._timeout
        try:
            if _is_async(call):
                result = call()
            else:
                result = await asyncio.wait_for(asyncio.shield(_in_thread(call)), self._timeout)
            if inspect.isawaitable(result):
                await asyncio.wait_for(result, max(deadline - time.monotonic(), 0))
        except asyncio.TimeoutError:
            logger.warning(
                "[GatewayExtensions] package %s %s() did not end within %.1f s; going on without it",
                package_id, hook, self._timeout,
            )
        except Exception:  # noqa: BLE001 - a broken hook must not fail management or shutdown
            logger.exception("[GatewayExtensions] package %s %s() failed", package_id, hook)

    def _call_blocking(self, package_id: str, hook: str, call: Callable[[], Any]) -> None:
        """Run one hook with no event loop running (a synchronous caller)."""

        if _is_async(call):
            try:
                asyncio.run(self._call(package_id, hook, call))
            except Exception:  # noqa: BLE001 - see _call
                logger.exception("[GatewayExtensions] package %s %s() failed", package_id, hook)
            return
        errors: list[BaseException] = []

        def target() -> None:
            try:
                call()
            except BaseException as exc:  # noqa: BLE001 - reported below
                errors.append(exc)

        thread = threading.Thread(target=target, name=f"extension-{hook}-{package_id}", daemon=True)
        thread.start()
        thread.join(self._timeout)
        if thread.is_alive():
            logger.warning(
                "[GatewayExtensions] package %s %s() did not end within %.1f s; going on without it",
                package_id, hook, self._timeout,
            )
        elif errors:
            logger.error("[GatewayExtensions] package %s %s() failed: %r", package_id, hook, errors[0])


def _hooks(package: Any, hook: str) -> list[Callable[[], Any]]:
    calls: list[Callable[[], Any]] = []
    for extension in getattr(package, "extensions", ()):
        call = getattr(extension, hook, None)
        if callable(call):
            calls.append(call)
    return calls


def _is_async(call: Callable[[], Any]) -> bool:
    return inspect.iscoroutinefunction(call) or inspect.iscoroutinefunction(getattr(call, "__call__", None))


def _in_thread(call: Callable[[], Any]) -> asyncio.Future[Any]:
    """Run a plain hook in a daemon thread; the future ends when it returns."""

    loop = asyncio.get_running_loop()
    future: asyncio.Future[Any] = loop.create_future()

    def settle(result: Any, error: BaseException | None) -> None:
        if future.done():
            return
        if error is None:
            future.set_result(result)
        else:
            future.set_exception(error)

    def target() -> None:
        try:
            result, error = call(), None
        except Exception as exc:  # noqa: BLE001 - handed to the loop
            result, error = None, exc
        try:
            loop.call_soon_threadsafe(settle, result, error)
        except RuntimeError:  # the loop has closed; nobody waits any more
            pass

    threading.Thread(target=target, name=f"extension-hook-{id(call):x}", daemon=True).start()
    return future
