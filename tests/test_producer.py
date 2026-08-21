from __future__ import annotations

from typing import Any

import pytest

import producer.producer as producer_module
from producer.producer import (
    inject_duplicate_events,
    load_vendor_payment_events,
    main,
    produce_events,
)


class FakeDeliveryFuture:
    """Minimal Kafka delivery future for producer unit tests."""

    def __init__(
        self,
        should_fail: bool = False,
    ) -> None:
        self.should_fail = should_fail
        self.timeout_received: int | None = None

    def get(
        self,
        timeout: int,
    ) -> dict[str, Any]:
        self.timeout_received = timeout

        if self.should_fail:
            raise RuntimeError("Kafka acknowledgement failed")

        return {
            "topic": "test-vendor-payments-topic",
            "partition": 0,
            "offset": 1,
        }


class FakeKafkaProducer:
    """Minimal Kafka producer replacement for unit tests."""

    def __init__(
        self,
        delivery_outcomes: list[bool] | None = None,
        fail_send_indexes: set[int] | None = None,
    ) -> None:
        self.delivery_outcomes = delivery_outcomes or []
        self.fail_send_indexes = fail_send_indexes or set()

        self.sent_messages: list[dict[str, Any]] = []
        self.delivery_futures: list[FakeDeliveryFuture] = []

        self.flush_called = False
        self.close_called = False
        self.send_count = 0

    def send(
        self,
        topic: str,
        key: bytes,
        value: dict[str, Any],
    ) -> FakeDeliveryFuture:
        current_index = self.send_count
        self.send_count += 1

        if current_index in self.fail_send_indexes:
            raise RuntimeError("Kafka send request failed")

        should_fail = (
            self.delivery_outcomes[current_index]
            if current_index < len(self.delivery_outcomes)
            else False
        )

        delivery_future = FakeDeliveryFuture(
            should_fail=should_fail,
        )

        self.sent_messages.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
            }
        )
        self.delivery_futures.append(delivery_future)

        return delivery_future

    def flush(self) -> None:
        self.flush_called = True

    def close(self) -> None:
        self.close_called = True


def build_event(
    event_id: str,
    business_composite_key: str | None = None,
    source_row_hash: str | None = None,
) -> dict[str, Any]:
    """Build a lightweight vendor-payment event for tests."""
    return {
        "event_id": event_id,
        "event_type": "vendor_payment_event",
        "event_timestamp": "2026-06-19T00:00:00+00:00",
        "source_system": "vendor_payments_etl_silver",
        "business_composite_key": business_composite_key,
        "source_row_hash": source_row_hash,
    }


def test_inject_duplicate_events_adds_configured_duplicates():
    events = [
        build_event(f"event-{index}")
        for index in range(100)
    ]

    produced_events = inject_duplicate_events(
        events=events,
        duplicate_rate=0.05,
        random_seed=42,
    )

    assert len(events) == 100
    assert len(produced_events) == 105

    unique_event_ids = {
        event["event_id"]
        for event in produced_events
    }

    assert len(unique_event_ids) == 100


def test_inject_duplicate_events_is_reproducible():
    events = [
        build_event(f"event-{index}")
        for index in range(20)
    ]

    first_result = inject_duplicate_events(
        events=events,
        duplicate_rate=0.25,
        random_seed=42,
    )
    second_result = inject_duplicate_events(
        events=events,
        duplicate_rate=0.25,
        random_seed=42,
    )

    first_event_ids = [
        event["event_id"]
        for event in first_result
    ]
    second_event_ids = [
        event["event_id"]
        for event in second_result
    ]

    assert first_event_ids == second_event_ids


def test_inject_duplicate_events_returns_empty_list():
    produced_events = inject_duplicate_events(
        events=[],
        duplicate_rate=0.05,
    )

    assert produced_events == []


def test_load_vendor_payment_events_returns_events_and_source_count(
    monkeypatch,
    tmp_path,
):
    source_file = tmp_path / "stream_sample.csv"

    source_file.write_text(
        (
            "event_id,event_type,event_timestamp,"
            "source_system,supplier_name,vouchers_paid\n"
            "event-001,vendor_payment_event,"
            "2026-06-19T00:00:00+00:00,"
            "vendor_payments_etl_silver,"
            "Supplier A,100.0\n"
            "event-002,vendor_payment_event,"
            "2026-06-19T00:00:01+00:00,"
            "vendor_payments_etl_silver,"
            "Supplier B,200.0\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        producer_module,
        "STREAM_SAMPLE_FILE",
        source_file,
    )

    events, source_row_count = load_vendor_payment_events()

    assert source_row_count == 2
    assert len(events) == 2
    assert events[0]["event_id"] == "event-001"
    assert events[1]["event_id"] == "event-002"


def test_load_vendor_payment_events_rejects_missing_file(
    monkeypatch,
    tmp_path,
):
    missing_file = tmp_path / "missing.csv"

    monkeypatch.setattr(
        producer_module,
        "STREAM_SAMPLE_FILE",
        missing_file,
    )

    with pytest.raises(
        FileNotFoundError,
        match="Streaming sample file not found",
    ):
        load_vendor_payment_events()


def test_produce_events_collects_successful_acknowledgements(
    monkeypatch,
):
    events = [
        build_event(
            event_id="event-001",
            business_composite_key="business-001",
        ),
        build_event(
            event_id="event-002",
            source_row_hash="hash-002",
        ),
    ]

    fake_producer = FakeKafkaProducer(
        delivery_outcomes=[False, False],
    )

    monkeypatch.setattr(
        producer_module,
        "build_kafka_producer",
        lambda: fake_producer,
    )

    metrics = produce_events(
        events=events,
        acknowledgement_timeout_seconds=15,
    )

    assert metrics == {
        "events_attempted": 2,
        "events_acknowledged": 2,
        "failed_events": 0,
    }

    assert len(fake_producer.sent_messages) == 2

    assert fake_producer.sent_messages[0]["key"] == (
        b"business-001"
    )
    assert fake_producer.sent_messages[1]["key"] == (
        b"hash-002"
    )

    assert all(
        future.timeout_received == 15
        for future in fake_producer.delivery_futures
    )

    assert fake_producer.flush_called is True
    assert fake_producer.close_called is True


def test_produce_events_counts_send_and_acknowledgement_failures(
    monkeypatch,
):
    events = [
        build_event("event-001"),
        build_event("event-002"),
        build_event("event-003"),
    ]

    fake_producer = FakeKafkaProducer(
        delivery_outcomes=[
            False,
            True,
            False,
        ],
        fail_send_indexes={2},
    )

    monkeypatch.setattr(
        producer_module,
        "build_kafka_producer",
        lambda: fake_producer,
    )

    metrics = produce_events(events)

    assert metrics == {
        "events_attempted": 3,
        "events_acknowledged": 1,
        "failed_events": 2,
    }

    assert fake_producer.flush_called is True
    assert fake_producer.close_called is True


def test_produce_events_closes_producer_when_send_fails(
    monkeypatch,
):
    events = [
        build_event("event-001"),
    ]

    fake_producer = FakeKafkaProducer(
        fail_send_indexes={0},
    )

    monkeypatch.setattr(
        producer_module,
        "build_kafka_producer",
        lambda: fake_producer,
    )

    metrics = produce_events(events)

    assert metrics["events_attempted"] == 1
    assert metrics["events_acknowledged"] == 0
    assert metrics["failed_events"] == 1
    assert fake_producer.close_called is True


def test_main_writes_producer_execution_report(
    monkeypatch,
    tmp_path,
):
    source_file = tmp_path / "stream_sample.csv"
    source_file.write_text(
        "event_id\n",
        encoding="utf-8",
    )

    base_events = [
        build_event("event-001"),
        build_event("event-002"),
    ]

    produced_events = [
        *base_events,
        base_events[0],
    ]

    captured_report_arguments: dict[str, Any] = {}
    saved_reports: list[dict[str, Any]] = []

    monkeypatch.setattr(
        producer_module,
        "STREAM_SAMPLE_FILE",
        source_file,
    )

    monkeypatch.setattr(
        producer_module,
        "load_vendor_payment_events",
        lambda: (base_events, 2),
    )

    monkeypatch.setattr(
        producer_module,
        "inject_duplicate_events",
        lambda events: produced_events,
    )

    monkeypatch.setattr(
        producer_module,
        "produce_events",
        lambda events: {
            "events_attempted": 3,
            "events_acknowledged": 3,
            "failed_events": 0,
        },
    )

    def fake_build_producer_execution_report(
        **kwargs,
    ) -> dict[str, Any]:
        captured_report_arguments.update(kwargs)

        return {
            "status": "success",
            "validation": {
                "status": "PASS",
            },
        }

    monkeypatch.setattr(
        producer_module,
        "build_producer_execution_report",
        fake_build_producer_execution_report,
    )

    monkeypatch.setattr(
        producer_module,
        "write_producer_execution_report",
        lambda report: saved_reports.append(report),
    )

    main()

    assert captured_report_arguments["source_file"] == (
        source_file
    )
    assert captured_report_arguments["source_row_count"] == 2
    assert captured_report_arguments["base_event_count"] == 2
    assert (
        captured_report_arguments["duplicate_events_injected"]
        == 1
    )
    assert captured_report_arguments["events_attempted"] == 3
    assert captured_report_arguments["events_acknowledged"] == 3
    assert captured_report_arguments["failed_events"] == 0
    assert captured_report_arguments["runtime_seconds"] >= 0
    assert (
        captured_report_arguments["duplicate_rate_configured"]
        == producer_module.DUPLICATE_RATE
    )
    assert (
        captured_report_arguments["topic"]
        == producer_module.TOPIC_VENDOR_PAYMENTS
    )

    assert saved_reports == [
        {
            "status": "success",
            "validation": {
                "status": "PASS",
            },
        }
    ]


def test_produce_events_resolves_acknowledgements_in_batches(
    monkeypatch,
):
    events = [
        build_event(f"event-{index}")
        for index in range(5)
    ]

    fake_producer = FakeKafkaProducer(
        delivery_outcomes=[False] * 5,
    )

    monkeypatch.setattr(
        producer_module,
        "build_kafka_producer",
        lambda: fake_producer,
    )

    metrics = produce_events(
        events=events,
        acknowledgement_batch_size=2,
    )

    assert metrics == {
        "events_attempted": 5,
        "events_acknowledged": 5,
        "failed_events": 0,
    }

    assert fake_producer.close_called is True
