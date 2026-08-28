from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common.config import (
    DEDUP_TTL_SECONDS,
    PRODUCER_EXECUTION_REPORT_FILE,
    STREAMING_SUMMARY_REPORT_FILE,
    TOPIC_VENDOR_PAYMENTS,
)


PIPELINE_VERSION = "1.0.0"
PROJECT_NAME = "Vendor Payments Kafka Streaming"


def calculate_duplicate_rate(
    consumed_events: int,
    rejected_duplicates: int,
) -> float:
    """Calculate the observed duplicate rate from consumed Kafka events."""
    if consumed_events == 0:
        return 0.0

    return rejected_duplicates / consumed_events


def count_jsonl_records(file_path: Path) -> int:
    """Count non-empty records in a JSONL output file."""
    if not file_path.exists():
        return 0

    with file_path.open("r", encoding="utf-8") as file:
        return sum(1 for line in file if line.strip())


def build_event_balance_validation(
    consumed_events: int,
    accepted_events: int,
    rejected_duplicates: int,
    failed_events: int,
) -> dict[str, Any]:
    """Validate that every consumed event has a processing outcome."""
    actual_events = (
        accepted_events
        + rejected_duplicates
        + failed_events
    )

    status = (
        "PASS"
        if consumed_events == actual_events
        else "FAIL"
    )

    return {
        "status": status,
        "expected": consumed_events,
        "actual": actual_events,
    }


def build_staging_count_validation(
    accepted_events: int,
    staging_record_count: int,
) -> dict[str, Any]:
    """Validate staging records against accepted events for the execution."""
    status = (
        "PASS"
        if accepted_events == staging_record_count
        else "FAIL"
    )

    return {
        "status": status,
        "expected": accepted_events,
        "actual": staging_record_count,
    }


def determine_pipeline_status(
    failed_events: int,
    validation_status: str,
) -> str:
    """Determine the final consumer execution status."""
    if validation_status == "FAIL":
        return "failed"

    if failed_events > 0:
        return "success_with_failures"

    return "success"


def build_producer_send_validation(
    events_attempted: int,
    events_acknowledged: int,
    failed_events: int,
) -> dict[str, Any]:
    """Validate that every attempted event has a delivery outcome."""
    actual_events = events_acknowledged + failed_events

    status = (
        "PASS"
        if events_attempted == actual_events
        else "FAIL"
    )

    return {
        "status": status,
        "expected": events_attempted,
        "actual": actual_events,
    }


def build_producer_input_validation(
    source_row_count: int,
    base_event_count: int,
) -> dict[str, Any]:
    """Validate that every source row was converted into a base event."""
    status = (
        "PASS"
        if source_row_count == base_event_count
        else "FAIL"
    )

    return {
        "status": status,
        "expected": source_row_count,
        "actual": base_event_count,
    }


def build_producer_execution_report(
    source_file: Path,
    source_row_count: int,
    base_event_count: int,
    duplicate_events_injected: int,
    events_attempted: int,
    events_acknowledged: int,
    failed_events: int,
    runtime_seconds: float,
    duplicate_rate_configured: float,
    topic: str = TOPIC_VENDOR_PAYMENTS,
) -> dict[str, Any]:
    """Build machine-readable metadata for a producer execution."""
    input_validation = build_producer_input_validation(
        source_row_count=source_row_count,
        base_event_count=base_event_count,
    )

    send_validation = build_producer_send_validation(
        events_attempted=events_attempted,
        events_acknowledged=events_acknowledged,
        failed_events=failed_events,
    )

    validation_status = (
        "PASS"
        if (
            input_validation["status"] == "PASS"
            and send_validation["status"] == "PASS"
        )
        else "FAIL"
    )

    status = determine_pipeline_status(
        failed_events=failed_events,
        validation_status=validation_status,
    )

    observed_duplicate_rate = (
        duplicate_events_injected / events_attempted
        if events_attempted > 0
        else 0.0
    )

    return {
        "project": PROJECT_NAME,
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "execution_scope": "producer",
        "status": status,
        "runtime_seconds": round(runtime_seconds, 3),
        "configuration": {
            "topic": topic,
            "duplicate_rate_configured": duplicate_rate_configured,
        },
        "input": {
            "source_file": str(source_file),
            "source_row_count": source_row_count,
            "available": source_file.exists(),
        },
        "producer": {
            "base_event_count": base_event_count,
            "duplicate_events_injected": duplicate_events_injected,
            "events_attempted": events_attempted,
            "events_acknowledged": events_acknowledged,
            "failed_events": failed_events,
            "observed_duplicate_rate": round(
                observed_duplicate_rate,
                4,
            ),
        },
        "validation": {
            "input_event_count": input_validation,
            "send_outcome_balance": send_validation,
            "status": validation_status,
        },
    }


def write_producer_execution_report(
    report: dict[str, Any],
    report_file: Path = PRODUCER_EXECUTION_REPORT_FILE,
) -> None:
    """Write producer execution metadata to a JSON file."""
    report_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_file.write_text(
        json.dumps(
            report,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def build_streaming_summary_report(
    consumed_events: int,
    accepted_events: int,
    rejected_duplicates: int,
    failed_events: int,
    large_payment_alerts_sent: int,
    runtime_seconds: float,
    consumer_group: str,
    staging_files: set[Path],
    topic: str = TOPIC_VENDOR_PAYMENTS,
) -> dict[str, Any]:
    """Build machine-readable metadata for a consumer execution."""
    staging_record_count = sum(
        count_jsonl_records(file_path)
        for file_path in staging_files
    )

    event_balance_validation = build_event_balance_validation(
        consumed_events=consumed_events,
        accepted_events=accepted_events,
        rejected_duplicates=rejected_duplicates,
        failed_events=failed_events,
    )

    staging_count_validation = build_staging_count_validation(
        accepted_events=accepted_events,
        staging_record_count=staging_record_count,
    )

    validation_status = (
        "PASS"
        if (
            event_balance_validation["status"] == "PASS"
            and staging_count_validation["status"] == "PASS"
        )
        else "FAIL"
    )

    duplicate_rate = calculate_duplicate_rate(
        consumed_events=consumed_events,
        rejected_duplicates=rejected_duplicates,
    )

    pipeline_status = determine_pipeline_status(
        failed_events=failed_events,
        validation_status=validation_status,
    )

    return {
        "project": PROJECT_NAME,
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "execution_scope": "consumer",
        "status": pipeline_status,
        "runtime_seconds": round(runtime_seconds, 3),
        "configuration": {
            "topic": topic,
            "consumer_group": consumer_group,
            "dedup_ttl_seconds": DEDUP_TTL_SECONDS,
        },
        "consumer": {
            "events_consumed": consumed_events,
            "accepted_events": accepted_events,
            "rejected_duplicates": rejected_duplicates,
            "failed_events": failed_events,
            "large_payment_alerts_sent": large_payment_alerts_sent,
        },
        "deduplication": {
            "architecture_layer_count": 2,
            "executed_layer_count": 1,
            "current_stage": {
                "layer": 1,
                "stage": "kafka_consumer",
                "strategy": "redis_event_id_deduplication",
                "duplicate_event_count": rejected_duplicates,
                "observed_duplicate_rate": round(
                    duplicate_rate,
                    4,
                ),
            },
            "downstream_stage": {
                "layer": 2,
                "stage": "airflow_downstream_processing",
                "included_in_this_execution": False,
            },
        },
        "outputs": {
            "staging": {
                "files": [
                    str(file_path)
                    for file_path in sorted(staging_files)
                ],
                "file_count": len(staging_files),
                "row_count": staging_record_count,
                "available": (
                    bool(staging_files)
                    and all(
                        file_path.exists()
                        for file_path in staging_files
                    )
                ),
            },
        },
        "validation": {
            "event_balance": event_balance_validation,
            "staging_record_count": staging_count_validation,
            "status": validation_status,
        },
    }


def write_streaming_summary_report(
    report: dict[str, Any],
    report_file: Path = STREAMING_SUMMARY_REPORT_FILE,
) -> None:
    """Write streaming execution metadata to a JSON file."""
    report_file.parent.mkdir(parents=True, exist_ok=True)

    report_file.write_text(
        json.dumps(
            report,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )