# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Registration surface handed to one Gateway extension package.

Gateways extensions may only register USER handlers: they observe and replace
platform behaviour, they never own a CORE protocol rule.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from agent_gateway.common.event_names import event_name_text
from agent_gateway.extensions_v2.models import EVENT_GATEWAY_REQUEST

ALLOWED_LEVELS = frozenset({"user"})

#: A failed ``gateway.request`` handler lets the frame continue as if the handler were absent.
ON_FAILURE_CONTINUE = "continue"
#: A failed ``gateway.request`` handler refuses the frame with ``EXTENSION_FAILED``.
ON_FAILURE_BLOCK = "block"
ON_FAILURE_VALUES = frozenset({ON_FAILURE_CONTINUE, ON_FAILURE_BLOCK})


@dataclass(frozen=True, slots=True)
class HandlerDeclaration:
    """One handler collected while loading a package; not on the bus yet."""

    event_type: str
    handler: Callable[..., Any]
    match_pattern: Mapping[str, Any]
    local_id: str
    #: What a frame does when this handler fails: raises, returns an unusable result or is still
    #: running when the request timeout ends. Only ``gateway.request`` handlers may block.
    on_failure: str = ON_FAILURE_CONTINUE


class GatewayRegistrar:
    """Collect handler declarations for one package activation."""

    def __init__(self) -> None:
        self.declarations: list[HandlerDeclaration] = []

    def register_event_handler(
        self,
        event_type: object,
        handler: Callable[..., Any],
        match_pattern: object = "*",
        handler_level: object = "user",
        *,
        on_failure: str = ON_FAILURE_CONTINUE,
    ) -> str:
        """Declare one USER handler and return its package-local id.

        ``match_pattern="*"`` is accepted as a shorthand for "match every
        event"; every other pattern must be a mapping so the same subset rule
        as AgentServer applies.

        ``on_failure`` says what a ``gateway.request`` frame does when this
        handler fails. ``"continue"`` (the default) passes the frame on as if
        the handler were absent. ``"block"`` refuses the frame, so a policy
        that did not decide is not bypassed. A ``match_pattern`` such as
        ``{"method": "chat.send"}`` limits the handler, and so the policy, to
        some methods.
        """

        level = _level_name(handler_level)
        if level not in ALLOWED_LEVELS:
            raise ValueError(f"gateway extensions may register user handlers only, got {level!r}")
        if not callable(handler):
            raise ValueError("handler must be callable")
        pattern = _match_pattern(match_pattern)
        if on_failure not in ON_FAILURE_VALUES:
            raise ValueError(f"on_failure must be 'continue' or 'block', got {on_failure!r}")
        local_id = f"local-{len(self.declarations) + 1}"
        # Registration accepts any object; the str() fallback keeps that contract
        # while enums are normalised to their wire name.
        event_name = event_name_text(event_type) or str(event_type)
        if on_failure == ON_FAILURE_BLOCK and event_name != EVENT_GATEWAY_REQUEST:
            raise ValueError(f"on_failure='block' applies to {EVENT_GATEWAY_REQUEST} handlers only")
        self.declarations.append(
            HandlerDeclaration(
                event_type=event_name,
                handler=handler,
                match_pattern=pattern,
                local_id=local_id,
                on_failure=on_failure,
            )
        )
        return local_id


def _match_pattern(value: object) -> Mapping[str, Any]:
    if value == "*":
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("match_pattern must be '*' or a mapping")
    return dict(value)


def _level_name(value: object) -> str:
    if isinstance(value, str):
        return value.strip().lower()
    inner = getattr(value, "value", None)
    if isinstance(inner, str):
        return inner.strip().lower()
    return str(value).strip().lower()
