from types import SimpleNamespace

import pytest

import consumer.consumer as consumer_module
from consumer.consumer import (
    consume_vendor_payment_events,
    validate_event,
)


def build_message(
    event: dict,
    offset: int,
) -> SimpleNamespace:
    """Build a lightweight Kafka message for consumer unit tests."""
    return SimpleNamespace(
        value=event,
        topic="test-vendor-payments-topic",
        partition=0,
        offset=offset,
    )


class FakeKafkaConsumer:
    """Minimal Kafka consumer replacement for unit tests."""

    def __init__(self, messages):
        self.messages = messages
        self.commit_count = 0
        self.closed = False

    def __iter__(self):
        return iter(self.messages)

    def commit(self):
        self.commit_count += 1

    def close(self):
        self.closed = True


class FakeRedisDeduplicator:
    """Minimal Redis deduplicator replacement for unit tests."""

    def __init__(self):
        self.marked_event_ids = []

    def is_duplicate(self, event_id: str) -> bool:
        return event_id == "event-duplicate"

    def mark_processed(self, event: dict) -> None:
        self.marked_event_ids.append(event["event_id"])


def test_validate_event_accepts_required_fields():
    event = {
        "event_id": "event-001",
        "event_type": "vendor_payment_event",
        "event_timestamp": "2026-06-06T00:00:00+00:00",
        "source_system": "vendor_payments_etl_silver",
        "window_id": "stream_window_001",
    }

    validate_event(event)


def test_validate_event_rejects_missing_event_id():
    event = {
        "event_type": "vendor_payment_event",
        "event_timestamp": "2026-06-06T00:00:00+00:00",
        "source_system": "vendor_payments_etl_silver",
    }

    with pytest.raises(
        ValueError,
        match="Missing required event fields",
    ):
        validate_event(event)


def test_validate_event_rejects_empty_required_field():
    event = {
        "event_id": "",
        "event_type": "vendor_payment_event",
        "event_timestamp": "2026-06-06T00:00:00+00:00",
        "source_system": "vendor_payments_etl_silver",
    }

    with pytest.raises(
        ValueError,
        match="Missing required event fields",
    ):
        validate_event(event)


def test_consume_vendor_payment_events_returns_execution_metrics(
    monkeypatch,
    tmp_path,
):
    accepted_event = {
        "event_id": "event-accepted",
        "event_type": "vendor_payment_event",
        "event_timestamp": "2026-06-06T00:00:00+00:00",
        "source_system": "vendor_payments_etl_silver",
        "payment_amount": 100.0,
        "window_id": "stream_window_001",
    }

    duplicate_event = {
        "event_id": "event-duplicate",
        "event_type": "vendor_payment_event",
        "event_timestamp": "2026-06-06T00:00:01+00:00",
        "source_system": "vendor_payments_etl_silver",
        "payment_amount": 200.0,
        "window_id": "stream_window_001",
    }

    invalid_event = {
        "event_type": "vendor_payment_event",
        "event_timestamp": "2026-06-06T00:00:02+00:00",
        "source_system": "vendor_payments_etl_silver",
    }

    messages = [
        build_message(accepted_event, offset=0),
        build_message(duplicate_event, offset=1),
        build_message(invalid_event, offset=2),
    ]

    fake_consumer = FakeKafkaConsumer(messages)
    fake_deduplicator = FakeRedisDeduplicator()

    written_events = []
    report_arguments = {}
    saved_reports = []

    monkeypatch.setattr(
        consumer_module,
        "STAGING_DIR",
        tmp_path,
    )

    monkeypatch.setattr(
        consumer_module,
        "connect_consumer_with_retry",
        lambda consumer_group: fake_consumer,
    )

    monkeypatch.setattr(
        consumer_module,
        "RedisDeduplicator",
        lambda: fake_deduplicator,
    )

    monkeypatch.setattr(
        consumer_module,
        "write_event_to_staging",
        lambda event, staging_file: written_events.append(
            {
                "event": event,
                "staging_file": staging_file,
            }
        ),
    )

    monkeypatch.setattr(
        consumer_module,
        "is_large_payment_event",
        lambda event: False,
    )

    def fake_build_streaming_summary_report(**kwargs):
        report_arguments.update(kwargs)

        return {
            "status": "success_with_failures",
            "validation": {
                "status": "PASS",
            },
        }

    monkeypatch.setattr(
        consumer_module,
        "build_streaming_summary_report",
        fake_build_streaming_summary_report,
    )

    monkeypatch.setattr(
        consumer_module,
        "write_streaming_summary_report",
        lambda report: saved_reports.append(report),
    )

    metrics = consume_vendor_payment_events(
        consumer_name="test-consumer",
        consumer_group="test-consumer-group",
    )

    assert metrics == {
        "consumed_events": 3,
        "accepted_events": 1,
        "rejected_duplicates": 1,
        "failed_events": 1,
        "large_payment_alerts_sent": 0,
    }

    assert len(written_events) == 1

    assert written_events[0]["event"] == accepted_event

    assert written_events[0]["staging_file"] == (
            tmp_path
            / "stream_window_001"
            / "events.jsonl"
    )

    assert fake_deduplicator.marked_event_ids == [
        "event-accepted"
    ]

    assert fake_consumer.commit_count == 2
    assert fake_consumer.closed is True

    assert report_arguments["consumed_events"] == 3
    assert report_arguments["accepted_events"] == 1
    assert report_arguments["rejected_duplicates"] == 1
    assert report_arguments["failed_events"] == 1
    assert report_arguments["large_payment_alerts_sent"] == 0
    assert report_arguments["consumer_group"] == (
        "test-consumer-group"
    )
    assert report_arguments["staging_files"] == {
        tmp_path
        / "stream_window_001"
        / "events.jsonl"
    }
    assert report_arguments["runtime_seconds"] >= 0

    assert saved_reports == [
        {
            "status": "success_with_failures",
            "validation": {
                "status": "PASS",
            },
        }
    ]