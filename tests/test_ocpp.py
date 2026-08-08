"""OCPPClient: the session handle, the flat primitives, frame encoding and
the caps, plus inert-on-bad-config.
"""

from __future__ import annotations

import base64
import logging
import uuid

import pytest
from conftest import MockUpstream, wait_for

from evpanda.config import OCPPConfig
from evpanda.identity import ChargerIdentity
from evpanda.ocpp import OCPPClient, OCPPMessageInput
from evpanda.types import OCPPDirection, OCPPEventType

IDENTITY = ChargerIdentity(charger_id="CP-001", tenant_id="t1", tenant_name="Tenant One")

OCPP_PATH = "/v1/ocpp"


def records(mock: MockUpstream) -> list[dict[str, object]]:
    return mock.records(OCPP_PATH)


def test_session_captures_connect_message_and_disconnect(mock: MockUpstream) -> None:
    client = OCPPClient.start(OCPPConfig(endpoint=mock.url, api_key="k", flush_interval=0.1))
    try:
        session = client.connection(IDENTITY)
        uuid.UUID(session.connection_id)  # SDK-minted, parseable
        session.message('[2,"id","BootNotification",{}]', OCPPDirection.FROM_CP)
        session.message(b'[3,"id",{}]', OCPPDirection.TO_CP)
        session.disconnect()

        wait_for(lambda: len(records(mock)) == 4, 3.0)
        recs = records(mock)

        assert [r["event_type"] for r in recs] == [
            OCPPEventType.CONNECT,
            OCPPEventType.MESSAGE,
            OCPPEventType.MESSAGE,
            OCPPEventType.DISCONNECT,
        ]
        assert {r["connection_id"] for r in recs} == {session.connection_id}
        assert {r["charger_id"] for r in recs} == {"CP-001"}
        assert recs[0]["direction"] is None  # connect carries no frame
        assert recs[0]["raw_frame"] is None
        assert recs[1]["direction"] == "FROM_CP"
        assert base64.standard_b64decode(str(recs[1]["raw_frame"])) == (
            b'[2,"id","BootNotification",{}]'
        )
        assert recs[2]["direction"] == "TO_CP"
        assert recs[3]["event_type"] == OCPPEventType.DISCONNECT
        assert recs[0]["tenant_id"] == "t1"
    finally:
        client.close()


def test_session_is_a_context_manager(mock: MockUpstream) -> None:
    client = OCPPClient.start(OCPPConfig(endpoint=mock.url, api_key="k", flush_interval=0.1))
    try:
        with client.connection(IDENTITY) as session:
            session.message(b"frame", OCPPDirection.TO_CP)
        wait_for(lambda: len(records(mock)) == 3, 3.0)
        assert [r["event_type"] for r in records(mock)] == [
            OCPPEventType.CONNECT,
            OCPPEventType.MESSAGE,
            OCPPEventType.DISCONNECT,
        ]
    finally:
        client.close()


def test_each_connection_gets_a_fresh_id(mock: MockUpstream) -> None:
    client = OCPPClient.start(OCPPConfig(endpoint=mock.url, api_key="k", flush_interval=60.0))
    try:
        assert (
            client.connection(IDENTITY).connection_id != client.connection(IDENTITY).connection_id
        )
    finally:
        client.close()


def test_message_requires_data_and_direction(mock: MockUpstream) -> None:
    client = OCPPClient.start(OCPPConfig(endpoint=mock.url, api_key="k", flush_interval=60.0))
    try:
        client.capture_message(OCPPMessageInput(identity=IDENTITY, connection_id="c1"))
        client.capture_message(
            OCPPMessageInput(identity=IDENTITY, connection_id="c1", data=b"frame")
        )
        client.capture_message(
            OCPPMessageInput(identity=IDENTITY, connection_id="c1", direction=OCPPDirection.TO_CP)
        )
        client.flush()
        assert records(mock) == []
    finally:
        client.close()


def test_oversize_frame_is_dropped(mock: MockUpstream) -> None:
    client = OCPPClient.start(
        OCPPConfig(endpoint=mock.url, api_key="k", max_capture_bytes=8, flush_interval=60.0)
    )
    try:
        session = client.connection(IDENTITY)
        session.message(b"x" * 9, OCPPDirection.TO_CP)  # over the cap ⇒ dropped
        session.message(b"y" * 8, OCPPDirection.TO_CP)  # exactly the cap ⇒ kept
        client.flush()
        wait_for(lambda: len(records(mock)) == 2, 3.0)

        frames = [r["raw_frame"] for r in records(mock) if r["event_type"] == OCPPEventType.MESSAGE]
        assert len(frames) == 1
        assert base64.standard_b64decode(str(frames[0])) == b"y" * 8
    finally:
        client.close()


def test_invalid_identity_is_dropped(mock: MockUpstream) -> None:
    client = OCPPClient.start(OCPPConfig(endpoint=mock.url, api_key="k", flush_interval=60.0))
    try:
        for bad in (
            ChargerIdentity(charger_id="  "),
            ChargerIdentity(charger_id="CP-1", tenant_id="t1"),  # half a tenant pair
        ):
            client.capture_connect(OCPPMessageInput(identity=bad, connection_id="c1"))
            client.capture_message(
                OCPPMessageInput(
                    identity=bad, connection_id="c1", data=b"f", direction=OCPPDirection.TO_CP
                )
            )
            client.capture_disconnect(OCPPMessageInput(identity=bad, connection_id="c1"))
        client.flush()
        assert records(mock) == []
    finally:
        client.close()


def test_close_goes_inert_and_is_idempotent(mock: MockUpstream) -> None:
    client = OCPPClient.start(OCPPConfig(endpoint=mock.url, api_key="k", flush_interval=60.0))
    session = client.connection(IDENTITY)
    client.close()
    client.close()

    # Post-close capture hits the inert engine — no delivery, no raise.
    before = len(records(mock))
    session.message(b"after", OCPPDirection.TO_CP)
    client.flush()
    assert len(records(mock)) == before


def test_bad_config_yields_an_inert_client(mock: MockUpstream) -> None:
    client = OCPPClient.start(OCPPConfig(endpoint=mock.url, api_key=""))
    with client.connection(IDENTITY) as session:
        session.message(b"frame", OCPPDirection.TO_CP)
    client.flush()
    client.close()
    assert records(mock) == []


def test_capture_faults_are_logged_not_raised(
    mock: MockUpstream, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="evpanda")
    client = OCPPClient.start(
        OCPPConfig(endpoint=mock.url, api_key="k", flush_interval=60.0, debug=True)
    )
    try:
        # `data` of a type the encoder can't handle — swallowed and logged.
        client.capture_message(
            OCPPMessageInput(
                identity=IDENTITY,
                connection_id="c1",
                data=12345,  # type: ignore[arg-type]
                direction=OCPPDirection.TO_CP,
            )
        )
        assert any("capture_message" in r.getMessage() for r in caplog.records)
    finally:
        client.close()
