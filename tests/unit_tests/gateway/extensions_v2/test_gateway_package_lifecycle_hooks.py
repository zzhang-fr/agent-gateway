# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Package ``start()`` and ``stop()`` hooks: uninstall, switch-off, version switch and shutdown stop a package."""

from __future__ import annotations

import json
import logging
import os
import time
import zipfile
from pathlib import Path

import pytest

from agent_gateway.extensions_v2 import config as gateway_config
from agent_gateway.extensions_v2.config import GatewayExtensionConfig, load_gateway_config
from agent_gateway.extensions_v2.loader import LoadedPackage
from agent_gateway.extensions_v2.management.registry import GatewayPackageManager
from agent_gateway.extensions_v2.registry import HandlerDeclaration
from agent_gateway.extensions_v2.runtime import GatewayRuntime


class Channel:
    channel_id = "web"

    def __init__(self) -> None:
        self.methods: dict[str, object] = {}

    def register_method(self, name: str, handler) -> None:
        self.methods[name] = handler

    async def ensure_ws_id(self, ws) -> str:
        return "ws-1"

    async def send_event(self, ws, event, payload) -> None:
        return None

    async def send_response(self, ws, req_id, *, ok, payload=None, error=None, code=None):
        return None


# Every hook appends "<hook> <version> <copy present>" to the log named by the entry.
HOOK_ENTRY = """\
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VERSION = json.loads((ROOT / "manifest.json").read_text())["package"]["version"]
LOG = Path({log!r})


def _note(text):
    with LOG.open("a", encoding="utf-8") as log:
        log.write(f"{{text}} {{VERSION}} {{(ROOT / 'manifest.json').exists()}}\\n")


class Extension:
    def register(self, registrar):
        async def handler(event, ops, next_handler):
            return await next_handler(event)

        registrar.register_event_handler("gateway.request", handler, {{}}, "user")
        _note("register")

    def start(self):
        _note("start")

    def stop(self):
        _note("stop")


def create_extension():
    return Extension()
"""

# start() runs a child process, as a scheduler or a local listener would; stop() ends it.
PROCESS_ENTRY = """\
import subprocess
import sys
from pathlib import Path

PID_FILE = Path({pid_file!r})


class Extension:
    def __init__(self):
        self.process = None

    def register(self, registrar):
        async def handler(event, ops, next_handler):
            return await next_handler(event)

        registrar.register_event_handler("gateway.request", handler, {{}}, "user")

    def start(self):
        self.process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
        PID_FILE.write_text(str(self.process.pid))

    def stop(self):
        self.process.terminate()
        self.process.wait(timeout=5)


def create_extension():
    return Extension()
"""

# start() starts an asyncio task on the Gateway's loop; an async stop() cancels and awaits it.
ASYNC_ENTRY = """\
import asyncio
from pathlib import Path

LOG = Path({log!r})


class Extension:
    def __init__(self):
        self.task = None

    def register(self, registrar):
        async def handler(event, ops, next_handler):
            return await next_handler(event)

        registrar.register_event_handler("gateway.request", handler, {{}}, "user")

    async def start(self):
        async def tick():
            try:
                while True:
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                LOG.write_text("cancelled")
                raise

        self.task = asyncio.create_task(tick())

    async def stop(self):
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)


def create_extension():
    return Extension()
"""

SLOW_STOP_ENTRY = """\
import time


class Extension:
    def register(self, registrar):
        async def handler(event, ops, next_handler):
            return await next_handler(event)

        registrar.register_event_handler("gateway.request", handler, {}, "user")

    def stop(self):
        time.sleep(10)


def create_extension():
    return Extension()
"""

FAILING_STOP_ENTRY = """\
class Extension:
    def register(self, registrar):
        async def handler(event, ops, next_handler):
            return await next_handler(event)

        registrar.register_event_handler("gateway.request", handler, {}, "user")

    def stop(self):
        raise RuntimeError("stop broke")


def create_extension():
    return Extension()
"""


def _package_zip(tmp_path: Path, entry: str, *, version: str = "0.1.0", package_id: str = "demo.hooks") -> Path:
    source = tmp_path / "src" / package_id / version
    source.mkdir(parents=True)
    manifest = {
        "package_type": "extension",
        "schemaVersion": 1,
        "package": {"id": package_id, "version": version, "domain": "gateway"},
        "extensions": [{"id": "demo", "runtime": "backend", "module": "extension.py"}],
    }
    (source / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (source / "extension.py").write_text(entry, encoding="utf-8")
    archive = tmp_path / f"{package_id}-{version}.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        for file in source.iterdir():
            bundle.write(file, file.name)
    return archive


async def _runtime(tmp_path: Path, *, lifecycle_timeout: float | None = None) -> GatewayRuntime:
    limit = {} if lifecycle_timeout is None else {"lifecycle_timeout": lifecycle_timeout}
    runtime = GatewayRuntime(
        channel=Channel(),
        config=GatewayExtensionConfig(
            install_dir=tmp_path / "extension_pkgs",
            data_dir=tmp_path / "extension" / "gateway",
            recovery_data_dir=tmp_path / "extension",
            **limit,
        ),
    )
    await runtime.start()
    return runtime


async def _manager(tmp_path: Path, runtime: GatewayRuntime, reported: bool) -> GatewayPackageManager:
    if not reported:
        return GatewayPackageManager(runtime, tmp_path / "extension_pkgs")
    manager = GatewayPackageManager(
        runtime,
        tmp_path / "extension_pkgs",
        report_path=tmp_path / "extension" / "extension_install_report.yaml",
        poll_interval=0.05,
        dependency_timeout=5.0,
        lock_timeout=2.0,
    )
    await manager.init()
    return manager


async def _close(manager: GatewayPackageManager, runtime: GatewayRuntime, reported: bool) -> None:
    if reported and not getattr(manager, "_test_closed", False):
        manager._test_closed = True
        await manager.close()
    await runtime.shutdown()


def _calls(log: Path) -> list[str]:
    return log.read_text(encoding="utf-8").splitlines() if log.exists() else []


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A terminated child the Gateway has not reaped yet is a zombie, not running work.
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as stat:
            return stat.read().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


REPORTED = pytest.mark.parametrize("reported", [False, True], ids=["legacy", "reported"])


@REPORTED
async def test_uninstall_calls_stop_once_before_the_copy_is_deleted(tmp_path, reported):
    log = tmp_path / "calls.log"
    runtime = await _runtime(tmp_path)
    manager = await _manager(tmp_path, runtime, reported)
    try:
        assert (await manager.install([_package_zip(tmp_path, HOOK_ENTRY.format(log=str(log)))]))[0].status == "SUCCESS"
        assert _calls(log) == ["register 0.1.0 True", "start 0.1.0 True"]

        report = await manager.uninstall("demo.hooks")

        assert report.status == "SUCCESS"
        assert _calls(log) == ["register 0.1.0 True", "start 0.1.0 True", "stop 0.1.0 True"]
        assert not (tmp_path / "extension_pkgs" / "demo.hooks" / "0.1.0").exists()
    finally:
        await _close(manager, runtime, reported)
    assert _calls(log).count("stop 0.1.0 True") == 1


@REPORTED
async def test_uninstalling_one_active_version_calls_stop_before_its_copy_goes(tmp_path, reported):
    log = tmp_path / "calls.log"
    runtime = await _runtime(tmp_path)
    manager = await _manager(tmp_path, runtime, reported)
    try:
        await manager.install([_package_zip(tmp_path, HOOK_ENTRY.format(log=str(log)))])

        report = await manager.uninstall("demo.hooks", "0.1.0")

        assert report.status == "SUCCESS"
        assert _calls(log)[-1] == "stop 0.1.0 True"
    finally:
        await _close(manager, runtime, reported)


@REPORTED
async def test_switch_off_calls_stop_and_switch_on_starts_once(tmp_path, reported):
    log = tmp_path / "calls.log"
    runtime = await _runtime(tmp_path)
    manager = await _manager(tmp_path, runtime, reported)
    try:
        await manager.install([_package_zip(tmp_path, HOOK_ENTRY.format(log=str(log)))])

        assert (await manager.set_enabled("demo.hooks", False)).status == "SUCCESS"
        assert _calls(log)[-1] == "stop 0.1.0 True"

        assert (await manager.set_enabled("demo.hooks", True)).status == "SUCCESS"
        calls = _calls(log)
        assert [call.split()[0] for call in calls] == ["register", "start", "stop", "register", "start"]
    finally:
        await _close(manager, runtime, reported)
    assert [call.split()[0] for call in _calls(log)][-1] == "stop"


@REPORTED
async def test_new_version_starts_after_the_old_version_stopped(tmp_path, reported):
    log = tmp_path / "calls.log"
    runtime = await _runtime(tmp_path)
    manager = await _manager(tmp_path, runtime, reported)
    try:
        await manager.install([_package_zip(tmp_path, HOOK_ENTRY.format(log=str(log)))])

        reports = await manager.install([_package_zip(tmp_path, HOOK_ENTRY.format(log=str(log)), version="0.2.0")])

        assert reports[0].status == "SUCCESS"
        calls = [" ".join(call.split()[:2]) for call in _calls(log)]
        # The reported path switches the old version off before it loads the new one; both stop it first.
        assert sorted(calls) == sorted(["register 0.1.0", "start 0.1.0", "register 0.2.0", "stop 0.1.0", "start 0.2.0"])
        assert calls.index("stop 0.1.0") < calls.index("start 0.2.0")
    finally:
        await _close(manager, runtime, reported)


async def test_reload_restarts_the_package(tmp_path):
    log = tmp_path / "calls.log"
    runtime = await _runtime(tmp_path)
    manager = await _manager(tmp_path, runtime, False)
    try:
        await manager.install([_package_zip(tmp_path, HOOK_ENTRY.format(log=str(log)))])

        assert (await manager.reload("demo.hooks")).status == "SUCCESS"

        assert [call.split()[0] for call in _calls(log)] == ["register", "start", "register", "stop", "start"]
    finally:
        await runtime.shutdown()


async def test_shutdown_waits_for_every_stop(tmp_path):
    log = tmp_path / "calls.log"
    runtime = await _runtime(tmp_path)
    manager = await _manager(tmp_path, runtime, False)
    await manager.install([_package_zip(tmp_path, HOOK_ENTRY.format(log=str(log)))])

    await runtime.shutdown()

    assert _calls(log)[-1] == "stop 0.1.0 True"
    assert _calls(log).count("stop 0.1.0 True") == 1


async def test_startup_load_calls_start_and_shutdown_calls_stop(tmp_path):
    log = tmp_path / "calls.log"
    first = await _runtime(tmp_path)
    manager = await _manager(tmp_path, first, False)
    await manager.install([_package_zip(tmp_path, HOOK_ENTRY.format(log=str(log)))])
    await first.shutdown()
    log.unlink()

    second = await _runtime(tmp_path)
    await second.settle_package("demo.hooks")
    assert [call.split()[0] for call in _calls(log)] == ["register", "start"]
    await second.shutdown()

    assert [call.split()[0] for call in _calls(log)] == ["register", "start", "stop"]


@REPORTED
@pytest.mark.parametrize("operation", ["uninstall", "disable", "shutdown"])
async def test_a_background_process_ends_with_the_package(tmp_path, reported, operation):
    pid_file = tmp_path / "worker.pid"
    runtime = await _runtime(tmp_path)
    manager = await _manager(tmp_path, runtime, reported)
    try:
        await manager.install([_package_zip(tmp_path, PROCESS_ENTRY.format(pid_file=str(pid_file)))])
        pid = int(pid_file.read_text())
        assert _alive(pid)

        if operation == "uninstall":
            assert (await manager.uninstall("demo.hooks")).status == "SUCCESS"
        elif operation == "disable":
            assert (await manager.set_enabled("demo.hooks", False)).status == "SUCCESS"
        else:
            await _close(manager, runtime, reported)

        assert not _alive(pid)
    finally:
        await _close(manager, runtime, reported)


async def test_async_hooks_run_on_the_gateway_loop(tmp_path):
    log = tmp_path / "async.log"
    runtime = await _runtime(tmp_path)
    manager = await _manager(tmp_path, runtime, False)
    try:
        await manager.install([_package_zip(tmp_path, ASYNC_ENTRY.format(log=str(log)))])
        assert not log.exists()

        assert (await manager.uninstall("demo.hooks")).status == "SUCCESS"

        assert log.read_text() == "cancelled"
    finally:
        await runtime.shutdown()


async def test_a_slow_stop_is_bounded_by_the_time_limit(tmp_path, caplog):
    runtime = await _runtime(tmp_path, lifecycle_timeout=0.3)
    manager = await _manager(tmp_path, runtime, False)
    try:
        await manager.install([_package_zip(tmp_path, SLOW_STOP_ENTRY)])
        began = time.monotonic()

        with caplog.at_level(logging.WARNING, logger="agent_gateway.extensions_v2.lifecycle"):
            report = await manager.uninstall("demo.hooks")

        assert report.status == "SUCCESS"
        assert time.monotonic() - began < 3
        assert "stop() did not end within 0.3 s" in caplog.text
        assert not (tmp_path / "extension_pkgs" / "demo.hooks").exists()
    finally:
        await runtime.shutdown()


async def test_a_failing_stop_fails_no_operation(tmp_path, caplog):
    runtime = await _runtime(tmp_path)
    manager = await _manager(tmp_path, runtime, False)
    try:
        await manager.install([_package_zip(tmp_path, FAILING_STOP_ENTRY)])

        with caplog.at_level(logging.ERROR, logger="agent_gateway.extensions_v2.lifecycle"):
            report = await manager.uninstall("demo.hooks")

        assert report.status == "SUCCESS"
        assert "stop() failed" in caplog.text
    finally:
        await runtime.shutdown()


async def test_a_package_without_hooks_is_unchanged(tmp_path):
    runtime = await _runtime(tmp_path)

    async def ready(event, ops, next_handler):
        return None

    runtime.publish(LoadedPackage(
        package_id="demo.plain", version="1.0.0", root=tmp_path,
        handlers=(HandlerDeclaration("gateway.request", ready, {}, "user"),),
        extensions=(object(),),
    ))
    assert runtime.unpublish("demo.plain")
    await runtime.settle_package("demo.plain")
    await runtime.shutdown()


def test_a_release_without_an_event_loop_still_calls_stop(tmp_path):
    calls: list[str] = []

    class Extension:
        def stop(self):
            calls.append("stop")

    runtime = GatewayRuntime(channel=Channel(), config=GatewayExtensionConfig(install_dir=tmp_path))
    prepared = runtime.prepare(LoadedPackage(
        package_id="demo.sync", version="1.0.0", root=tmp_path, handlers=(), extensions=(Extension(),),
    ))
    runtime.abort(prepared)
    runtime.abort(prepared)

    assert calls == ["stop"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, "default"), (2, 2.0), (0.5, 0.5), (0, "default"), (-1, "default"), (True, "default"),
     ("3", "default"), (float("nan"), "default"), (3600, "max")],
)
def test_lifecycle_timeout_setting(value, expected, tmp_path):
    limits = {"default": getattr(gateway_config, "DEFAULT_LIFECYCLE_TIMEOUT", None),
              "max": getattr(gateway_config, "MAX_LIFECYCLE_TIMEOUT", None)}
    gateway = {} if value is None else {"lifecycle_timeout": value}
    config = load_gateway_config({"extensions": {"install_dir": str(tmp_path), "gateway": gateway}})
    assert getattr(config, "lifecycle_timeout", None) == limits.get(expected, expected)
    assert limits["default"] == 5.0

