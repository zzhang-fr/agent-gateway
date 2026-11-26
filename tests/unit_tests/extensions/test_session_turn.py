# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""session.start_turn: the call by which an application plugin starts a turn in a Session."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from agent_gateway.common.session_message import with_cross_session_history_metadata
from agent_gateway.extensions.registry import ExtensionRegistry
from agent_gateway.extensions.sdk.application_plugin import (
    ApplicationPluginExtension,
    ApplicationPluginServices,
)
from agent_gateway.extensions.sdk.session_turn import SessionTurnError, start_session_turn


class _AgentClient:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.sent: list[Any] = []

    async def send_request(self, envelope: Any) -> Any:
        self.sent.append(envelope)
        return self.response


def _sent_method_and_params(envelope: Any) -> tuple[str, dict, str]:
    return str(envelope.method), dict(envelope.params or {}), str(envelope.session_id or "")


@pytest.mark.asyncio
async def test_start_session_turn_sends_the_request_and_returns_the_record():
    record = {"message_id": "sm-1", "status": "queued", "deduplicated": False}
    client = _AgentClient(SimpleNamespace(ok=True, payload={"result": record}))

    result = await start_session_turn(
        client,
        plugin_id="mail-watcher",
        session_id="sess-1",
        prompt="Summarize mail 8812 and draft a reply.",
        submission_id="mail-8812",
        display_text="New mail from Alice",
        agent_template_name="coding-expert",
        user_id="user-1",
    )

    assert result == record
    method, params, session_id = _sent_method_and_params(client.sent[0])
    assert method == "session.start_turn"
    assert session_id == "sess-1"
    assert client.sent[0].user_id == "user-1"
    assert {
        "plugin_id": "mail-watcher",
        "prompt": "Summarize mail 8812 and draft a reply.",
        "submission_id": "mail-8812",
        "display_text": "New mail from Alice",
        "agent_template_name": "coding-expert",
    }.items() <= params.items()


@pytest.mark.asyncio
async def test_start_session_turn_raises_the_agent_server_code():
    client = _AgentClient(
        SimpleNamespace(ok=False, payload={"code": "UNSUPPORTED_TARGET", "error": "cron Session"})
    )

    with pytest.raises(SessionTurnError) as excinfo:
        await start_session_turn(
            client, plugin_id="p", session_id="s", prompt="x", submission_id="1"
        )

    assert excinfo.value.code == "UNSUPPORTED_TARGET"


class _Plugin(ApplicationPluginExtension):
    def __init__(self, plugin_id: str) -> None:
        super().__init__()
        self.plugin_id = plugin_id
        self.services: ApplicationPluginServices | None = None

    async def initialize(self, config: Any) -> None:
        del config

    async def shutdown(self) -> None:
        return None

    def bind_web_channel(self, channel: Any, services: ApplicationPluginServices) -> None:
        del channel
        self.services = services


@pytest.mark.asyncio
async def test_each_plugin_starts_turns_under_its_own_id():
    client = _AgentClient(SimpleNamespace(ok=True, payload={"result": {"status": "queued"}}))
    registry = ExtensionRegistry(callback_framework=None, config={}, logger=None)
    mail, calendar = _Plugin("mail-watcher"), _Plugin("calendar")
    registry.register_application_plugin(mail)
    registry.register_application_plugin(calendar)

    registry.bind_application_plugins(SimpleNamespace(), agent_client=client)
    await mail.services.start_session_turn(session_id="s", prompt="x", submission_id="1")
    await calendar.services.start_session_turn(session_id="s", prompt="y", submission_id="1")

    assert [_sent_method_and_params(e)[1]["plugin_id"] for e in client.sent] == [
        "mail-watcher",
        "calendar",
    ]


@pytest.mark.asyncio
async def test_services_without_a_plugin_id_start_no_turn():
    client = _AgentClient(SimpleNamespace(ok=True, payload={"result": {}}))

    with pytest.raises(RuntimeError):
        await ApplicationPluginServices(agent_client=client).start_session_turn(
            session_id="s", prompt="x", submission_id="1"
        )
    assert client.sent == []


def test_history_keeps_the_plugin_and_its_display_text():
    extra = with_cross_session_history_metadata(
        None,
        {
            "message_id": "sm-1",
            "source_session_id": "plugin:mail-watcher",
            "source_title": "mail-watcher",
            "plugin_id": "mail-watcher",
            "display_text": "New mail from Alice",
        },
    )

    assert extra["cross_session"]["plugin_id"] == "mail-watcher"
    assert extra["cross_session"]["display_text"] == "New mail from Alice"
