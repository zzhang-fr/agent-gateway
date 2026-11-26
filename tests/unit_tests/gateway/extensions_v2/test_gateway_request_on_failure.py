# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""``on_failure`` of a ``gateway.request`` handler: a failed handler continues or blocks the frame."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent_gateway.extensions_v2 import runtime as runtime_module
from agent_gateway.extensions_v2.config import GatewayExtensionConfig
from agent_gateway.extensions_v2.loader import LoadedPackage
from agent_gateway.extensions_v2.models import EVENT_GATEWAY_REQUEST, GatewayRequestOutput
from agent_gateway.extensions_v2.registry import GatewayRegistrar, HandlerDeclaration
from agent_gateway.extensions_v2.runtime import GatewayRuntime


class FakeChannel:
    """Minimal stand-in for WebChannel used by the runtime."""

    channel_id = "web"

    def __init__(self) -> None:
        self.responses: list[dict] = []

    async def ensure_ws_id(self, ws) -> str:
        ws._jiuwen_ws_id = "ws-1"
        return "ws-1"

    def connection_user_id(self, ws):
        return None

    async def send_response(self, ws, req_id, *, ok, payload=None, error=None, code=None):
        self.responses.append({"id": req_id, "ok": ok, "error": error, "code": code})


def _package(handler, on_failure: str = "continue", match_pattern=None) -> LoadedPackage:
    return LoadedPackage(
        package_id="policy",
        version="1.0.0",
        root=Path("/tmp/policy"),
        handlers=(HandlerDeclaration(EVENT_GATEWAY_REQUEST, handler, match_pattern or {}, "local-1",
                                     **({} if on_failure == "continue" else {"on_failure": on_failure})),),
    )


@pytest.fixture
async def gateway(tmp_path):
    channel = FakeChannel()
    runtime = GatewayRuntime(
        channel=channel,
        config=GatewayExtensionConfig(install_dir=tmp_path, data_dir=tmp_path / "data"),
    )
    await runtime.start()
    yield runtime, channel
    await runtime.shutdown()


async def _raises(event, ops, next_handler):
    raise RuntimeError("the check failed before deciding")


async def _slow(event, ops, next_handler):
    await asyncio.sleep(1)
    return GatewayRequestOutput(handled=True, route="deny", error={"code": "DENIED"})


async def _unusable(event, ops, next_handler):
    return {"route": "deny"}


async def _delegates(event, ops, next_handler):
    return await next_handler(event)


# ── registration ──────────────────────────────────────────────────────────


def test_on_failure_defaults_to_continue():
    registrar = GatewayRegistrar()
    registrar.register_event_handler("gateway.request", _delegates, {}, "user")

    assert registrar.declarations[0].on_failure == "continue"


def test_on_failure_block_is_recorded():
    registrar = GatewayRegistrar()
    registrar.register_event_handler("gateway.request", _delegates, {"method": "chat.send"}, "user",
                                     on_failure="block")

    assert registrar.declarations[0].on_failure == "block"
    assert registrar.declarations[0].match_pattern == {"method": "chat.send"}


def test_on_failure_rejects_an_unknown_value():
    with pytest.raises(ValueError, match="on_failure"):
        GatewayRegistrar().register_event_handler("gateway.request", _delegates, {}, "user", on_failure="deny")


def test_on_failure_block_applies_to_gateway_request_only():
    with pytest.raises(ValueError, match="gateway.request"):
        GatewayRegistrar().register_event_handler("ui.ready", _delegates, {}, "user", on_failure="block")


# ── continue: today's behaviour ───────────────────────────────────────────


@pytest.mark.parametrize("handler", [_raises, _unusable])
async def test_continue_handler_failure_lets_the_frame_through(gateway, handler):
    runtime, channel = gateway
    runtime.publish(_package(handler, "continue"))

    visited, params = await runtime.intercept(SimpleNamespace(), "r1", "chat.send", {"content": "hi"})

    assert (visited, params) == (False, {"content": "hi"})
    assert channel.responses == []


async def test_continue_handler_timeout_lets_the_frame_through(gateway, monkeypatch):
    runtime, channel = gateway
    monkeypatch.setattr(runtime_module, "REQUEST_TIMEOUT_S", 0.05)
    runtime.publish(_package(_slow, "continue"))

    visited, params = await runtime.intercept(SimpleNamespace(), "r1", "chat.send", {"content": "hi"})

    assert (visited, params) == (False, {"content": "hi"})
    assert channel.responses == []


# ── block ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("handler", [_raises, _unusable])
async def test_block_handler_failure_refuses_the_frame(gateway, handler):
    runtime, channel = gateway
    runtime.publish(_package(handler, "block"))

    visited, _ = await runtime.intercept(SimpleNamespace(), "r1", "chat.send", {"content": "hi"})

    assert visited is True
    assert len(channel.responses) == 1
    assert channel.responses[0]["ok"] is False
    assert channel.responses[0]["code"] == "EXTENSION_FAILED"
    assert "policy" in channel.responses[0]["error"]


async def test_block_handler_timeout_refuses_the_frame(gateway, monkeypatch):
    runtime, channel = gateway
    monkeypatch.setattr(runtime_module, "REQUEST_TIMEOUT_S", 0.05)
    runtime.publish(_package(_slow, "block"))

    visited, _ = await runtime.intercept(SimpleNamespace(), "r1", "chat.send", {"content": "hi"})

    assert visited is True
    assert [(r["ok"], r["code"]) for r in channel.responses] == [(False, "EXTENSION_FAILED")]


async def test_block_handler_that_decides_keeps_its_decision(gateway):
    runtime, channel = gateway

    async def deny(event, ops, next_handler):
        return GatewayRequestOutput(handled=True, route="deny", error={"code": "DENIED", "message": "no"})

    runtime.publish(_package(deny, "block"))

    visited, _ = await runtime.intercept(SimpleNamespace(), "r1", "chat.send", {"content": "hi"})

    assert visited is True
    assert [(r["ok"], r["code"]) for r in channel.responses] == [(False, "DENIED")]


async def test_block_handler_that_delegates_lets_the_frame_through(gateway):
    runtime, channel = gateway
    runtime.publish(_package(_delegates, "block"))

    visited, params = await runtime.intercept(SimpleNamespace(), "r1", "chat.send", {"content": "hi"})

    assert (visited, params) == (False, {"content": "hi"})
    assert channel.responses == []


async def test_block_handler_limited_by_method_leaves_other_methods_alone(gateway):
    runtime, channel = gateway
    runtime.publish(_package(_raises, "block", {"method": "chat.send"}))

    visited, params = await runtime.intercept(SimpleNamespace(), "r1", "history.get", {"session_id": "s1"})
    assert (visited, params) == (False, {"session_id": "s1"})

    visited, _ = await runtime.intercept(SimpleNamespace(), "r2", "chat.send", {"content": "hi"})
    assert visited is True
    assert [(r["id"], r["code"]) for r in channel.responses] == [("r2", "EXTENSION_FAILED")]


async def test_a_later_frame_is_not_refused_by_an_earlier_timeout(gateway, monkeypatch):
    runtime, channel = gateway
    monkeypatch.setattr(runtime_module, "REQUEST_TIMEOUT_S", 0.05)
    calls = []

    async def slow_once(event, ops, next_handler):
        calls.append(event.event_input.request_id)
        if len(calls) == 1:
            await asyncio.sleep(1)
        return await next_handler(event)

    runtime.publish(_package(slow_once, "block"))

    first, _ = await runtime.intercept(SimpleNamespace(), "r1", "chat.send", {"content": "a"})
    second, params = await runtime.intercept(SimpleNamespace(), "r2", "chat.send", {"content": "b"})

    assert first is True
    assert (second, params) == (False, {"content": "b"})
    assert [r["id"] for r in channel.responses] == ["r1"]
