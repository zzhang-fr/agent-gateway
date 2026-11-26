# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""The call by which an application plugin starts a turn in a Session."""

from __future__ import annotations

import secrets
import time
from typing import Any

from e2a.gateway_normalize import message_to_e2a_or_fallback
from e2a.message import Message, ReqMethod

from agent_gateway.common import protocol_methods  # noqa: F401  registers SESSION_START_TURN

SESSION_START_TURN = "session.start_turn"


class SessionTurnError(RuntimeError):
    """AgentServer refused the turn; ``code`` is its error code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


async def start_session_turn(
    agent_client: Any,
    *,
    plugin_id: str,
    session_id: str,
    prompt: str,
    submission_id: str,
    display_text: str = "",
    agent_template_name: str = "",
    user_id: str = "",
) -> dict[str, Any]:
    """Queue one turn in ``session_id`` through the AgentServer's Session mailbox.

    The turn runs in the ``follow_up`` input mode: it waits while the Session
    is busy, and a message the person types meanwhile waits for it. The same
    ``submission_id`` of one plugin starts at most one turn; a repeat returns
    the existing record with ``deduplicated: true``. The chat shows
    ``display_text`` and the model gets ``prompt``. ``agent_template_name``
    names the expert that runs the turn.

    Returns the mailbox record: ``message_id``, ``status`` and
    ``deduplicated``. Raises ``SessionTurnError`` with AgentServer's code,
    such as ``INVALID_ARGUMENT``, ``NOT_FOUND_OR_FORBIDDEN`` or
    ``UNSUPPORTED_TARGET``; a refused call creates no record.
    """
    msg = Message(
        id=f"session-turn-{secrets.token_hex(8)}",
        type="req",
        channel_id="web",
        session_id=session_id,
        req_method=ReqMethod.SESSION_START_TURN,
        params={
            "plugin_id": plugin_id,
            "prompt": prompt,
            "submission_id": submission_id,
            "display_text": display_text,
            "agent_template_name": agent_template_name,
        },
        timestamp=time.time(),
        ok=True,
        is_stream=False,
        user_id=user_id or None,
    )
    response = await agent_client.send_request(message_to_e2a_or_fallback(msg))
    payload = response.payload if isinstance(response.payload, dict) else {}
    if not response.ok:
        raise SessionTurnError(
            str(payload.get("code") or "INTERNAL_ERROR"), str(payload.get("error") or "")
        )
    result = payload.get("result")
    return dict(result) if isinstance(result, dict) else {}


__all__ = ["SESSION_START_TURN", "SessionTurnError", "start_session_turn"]
